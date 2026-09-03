package auth

import (
	"context"
	"net/http"
	"sync"
	"testing"
	"time"

	internalconfig "github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/registry"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
)

// A fake upstream that answers per model. Records every (auth, model) it was asked for, in
// order, which is the whole proof: the fallback must appear only after the ordinary model
// was refused, and never while ordinary is serving.
type fallbackFakeExecutor struct {
	id      string
	mu      sync.Mutex
	replies map[string]error // model -> error (nil = 200)
	calls   []string         // "authID/model"
}

func (e *fallbackFakeExecutor) Identifier() string { return e.id }
func (e *fallbackFakeExecutor) record(auth *Auth, req cliproxyexecutor.Request) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.calls = append(e.calls, auth.ID+"/"+req.Model)
	return e.replies[req.Model]
}
func (e *fallbackFakeExecutor) Execute(ctx context.Context, auth *Auth, req cliproxyexecutor.Request, _ cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	cliproxyexecutor.MarkUpstreamAttempt(ctx)
	if err := e.record(auth, req); err != nil {
		return cliproxyexecutor.Response{}, err
	}
	return cliproxyexecutor.Response{Payload: []byte(`{"model":"` + req.Model + `"}`)}, nil
}
func (e *fallbackFakeExecutor) ExecuteStream(ctx context.Context, auth *Auth, req cliproxyexecutor.Request, _ cliproxyexecutor.Options) (*cliproxyexecutor.StreamResult, error) {
	cliproxyexecutor.MarkUpstreamAttempt(ctx)
	if err := e.record(auth, req); err != nil {
		return nil, err
	}
	ch := make(chan cliproxyexecutor.StreamChunk, 1)
	ch <- cliproxyexecutor.StreamChunk{Payload: []byte(`{"model":"` + req.Model + `"}`)}
	close(ch)
	return &cliproxyexecutor.StreamResult{Chunks: ch}, nil
}
func (*fallbackFakeExecutor) Refresh(_ context.Context, auth *Auth) (*Auth, error) { return auth, nil }
func (e *fallbackFakeExecutor) CountTokens(ctx context.Context, auth *Auth, req cliproxyexecutor.Request, opts cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	return e.Execute(ctx, auth, req, opts)
}
func (*fallbackFakeExecutor) HttpRequest(context.Context, *Auth, *http.Request) (*http.Response, error) {
	return nil, nil
}

// A 429 that carries a reset time, the way the codex executor surfaces usage_limit_reached
// (codex_executor_terminal.go parseCodexRetryAfter). The gateway keys its cooldown on this.
type quotaErr struct {
	inner *Error
	after time.Duration
}

func (q *quotaErr) Error() string              { return q.inner.Error() }
func (q *quotaErr) StatusCode() int            { return q.inner.HTTPStatus }
func (q *quotaErr) RetryAfter() *time.Duration { return &q.after }
func (q *quotaErr) Unwrap() error              { return q.inner }

func quota429() error {
	return &quotaErr{inner: &Error{HTTPStatus: http.StatusTooManyRequests, Message: `{"error":{"type":"usage_limit_reached"}}`}, after: 10 * time.Minute}
}

func newFallbackFixture(t *testing.T, provider string) (*Manager, *fallbackFakeExecutor, *Auth, *Auth) {
	t.Helper()
	// Mirror production: routing.strategy fill-first wrapped in session affinity. The built-in
	// selectors are handed model="" (selectionArgForSelector) and judge availability at auth
	// level, where any cooling model lifts Quota.Exceeded onto the whole credential; the
	// affinity selector passes the route model and judges per model. Every real codex request
	// takes the second path, so that is the one this suite must exercise.
	m := NewManager(nil, NewSessionAffinitySelectorWithConfig(SessionAffinityConfig{Fallback: &FillFirstSelector{}, TTL: time.Hour}), nil)
	m.SetRetryConfig(0, 0, 0)
	exec := &fallbackFakeExecutor{id: provider, replies: map[string]error{}}
	m.RegisterExecutor(exec)

	// entitled: carries the fallback alias. Priority 1000 so fill-first reaches it first.
	entitled := &Auth{ID: provider + "-entitled", Provider: provider, Attributes: map[string]string{"priority": "1000"}}
	SetFallbackModelsAttribute(entitled, []internalconfig.FallbackModel{{Name: "gpt-reserve", Alias: "gpt-5.6-luna"}})
	// plain: no fallback, lower priority. Its ordinary capacity must be spent before any
	// reserve request goes out.
	plain := &Auth{ID: provider + "-plain", Provider: provider, Attributes: map[string]string{"priority": "500"}}

	for _, a := range []*Auth{entitled, plain} {
		registry.GetGlobalRegistry().RegisterClient(a.ID, a.Provider, []*registry.ModelInfo{{ID: "gpt-5.6-luna"}})
		id := a.ID
		t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(id) })
		if _, err := m.Register(context.Background(), a); err != nil {
			t.Fatalf("register %s: %v", a.ID, err)
		}
	}
	return m, exec, entitled, plain
}

func TestFallbackAliasIsNeverSentWhileOrdinaryServes(t *testing.T) {
	m, exec, _, _ := newFallbackFixture(t, "fb-serving")
	for i := 0; i < 5; i++ {
		if _, err := m.Execute(context.Background(), []string{"fb-serving"}, cliproxyexecutor.Request{Model: "gpt-5.6-luna"}, cliproxyexecutor.Options{}); err != nil {
			t.Fatalf("execute %d: %v", i, err)
		}
	}
	for _, call := range exec.calls {
		if call == "fb-serving-entitled/gpt-reserve" {
			t.Fatalf("gpt-reserve was requested while ordinary luna was serving: %v", exec.calls)
		}
	}
}

func TestFallbackAliasIsSentOnlyAfterEveryOrdinaryCandidateCools(t *testing.T) {
	m, exec, _, _ := newFallbackFixture(t, "fb-drain")
	exec.replies["gpt-5.6-luna"] = quota429() // ordinary is dry on every account
	resp, err := m.Execute(context.Background(), []string{"fb-drain"}, cliproxyexecutor.Request{Model: "gpt-5.6-luna"}, cliproxyexecutor.Options{})
	if err != nil {
		t.Fatalf("expected the reserve to serve, got error: %v (calls=%v)", err, exec.calls)
	}
	if string(resp.Payload) != `{"model":"gpt-reserve"}` {
		t.Fatalf("expected reserve payload, got %s (calls=%v)", resp.Payload, exec.calls)
	}
	// Order is the acceptance criterion: entitled luna (429) -> plain luna (429) -> entitled reserve.
	// The reserve must be LAST, after the lower-priority plain account's ordinary attempt.
	want := []string{"fb-drain-entitled/gpt-5.6-luna", "fb-drain-plain/gpt-5.6-luna", "fb-drain-entitled/gpt-reserve"}
	if len(exec.calls) != len(want) {
		t.Fatalf("calls=%v want=%v", exec.calls, want)
	}
	for i := range want {
		if exec.calls[i] != want[i] {
			t.Fatalf("call %d = %s, want %s (all=%v)", i, exec.calls[i], want[i], exec.calls)
		}
	}
}

func TestFallbackAndOrdinaryCoolIndependently(t *testing.T) {
	m, exec, entitled, plain := newFallbackFixture(t, "fb-cool")
	exec.replies["gpt-5.6-luna"] = quota429()
	if _, err := m.Execute(context.Background(), []string{"fb-cool"}, cliproxyexecutor.Request{Model: "gpt-5.6-luna"}, cliproxyexecutor.Options{}); err != nil {
		t.Fatalf("first execute: %v", err)
	}
	now := time.Now()
	for _, a := range []*Auth{entitled, plain} {
		got, _ := m.GetByID(a.ID)
		// Strict: the ordinary upstream itself is cooling on both accounts.
		if blocked, _, _ := isAuthBlockedForModelStrict(got, "gpt-5.6-luna", now); !blocked {
			t.Fatalf("%s luna should be cooling", a.ID)
		}
	}
	got, _ := m.GetByID(entitled.ID)
	if blocked, _, _ := isAuthBlockedForModelStrict(got, "gpt-reserve", now); blocked {
		t.Fatalf("entitled gpt-reserve must NOT be cooling after a luna 429")
	}
	// And the fallback-aware verdict the selectors use: entitled still serves luna (through
	// the reserve), plain does not.
	if blocked, _, _ := isAuthBlockedForModel(got, "gpt-5.6-luna", now); blocked {
		t.Fatalf("entitled must remain selectable for luna while its reserve is open")
	}
	gotPlain, _ := m.GetByID(plain.ID)
	if blocked, _, _ := isAuthBlockedForModel(gotPlain, "gpt-5.6-luna", now); !blocked {
		t.Fatalf("plain has no fallback and must read as blocked")
	}
	// A second request goes straight to the reserve: ordinary is cooling, no upstream luna retry.
	before := len(exec.calls)
	if _, err := m.Execute(context.Background(), []string{"fb-cool"}, cliproxyexecutor.Request{Model: "gpt-5.6-luna"}, cliproxyexecutor.Options{}); err != nil {
		t.Fatalf("second execute: %v", err)
	}
	added := exec.calls[before:]
	if len(added) != 1 || added[0] != "fb-cool-entitled/gpt-reserve" {
		t.Fatalf("second request should hit only the reserve, got %v", added)
	}
}

func TestReserveExhaustionDoesNotBlockOrdinaryOnRecovery(t *testing.T) {
	m, exec, entitled, _ := newFallbackFixture(t, "fb-both")
	exec.replies["gpt-5.6-luna"] = quota429()
	exec.replies["gpt-reserve"] = quota429()
	if _, err := m.Execute(context.Background(), []string{"fb-both"}, cliproxyexecutor.Request{Model: "gpt-5.6-luna"}, cliproxyexecutor.Options{}); err == nil {
		t.Fatalf("both dry: expected an error")
	}
	got, _ := m.GetByID(entitled.ID)
	// Both cooling, separately keyed.
	if b, _, _ := isAuthBlockedForModel(got, "gpt-reserve", time.Now()); !b {
		t.Fatalf("reserve should be cooling")
	}
	if got.Quota.Exceeded && got.Quota.Reason == "credential_quota" {
		t.Fatalf("a model-scoped 429 pair must not escalate to a credential-wide quota block")
	}
}

func TestFallbackEntriesNeverBecomeTheOrdinaryUpstream(t *testing.T) {
	auth := &Auth{ID: "fb-alias", Provider: "codex"}
	SetFallbackModelsAttribute(auth, []internalconfig.FallbackModel{{Name: "gpt-reserve", Alias: "gpt-5.6-luna"}})
	if got := resolveUpstreamModelFromAliases(OAuthModelAliasesFromAttributes(auth.Attributes), "gpt-5.6-luna"); got.UpstreamModel != "" {
		t.Fatalf("a fallback-only alias must not rewrite the ordinary upstream, got %q", got.UpstreamModel)
	}
	if got := FallbackUpstreamModels(auth, "gpt-5.6-luna(xhigh)"); len(got) != 1 || got[0] != "gpt-reserve(xhigh)" {
		t.Fatalf("effort suffix must carry over to the fallback, got %v", got)
	}
	plain := &Auth{ID: "fb-none", Provider: "codex"}
	if got := FallbackUpstreamModels(plain, "gpt-5.6-luna"); len(got) != 0 {
		t.Fatalf("an account without the alias has no fallback, got %v", got)
	}
}

// The streaming path is the one Claude Code actually uses. Same ordering property.
func TestFallbackAliasOrderingHoldsForStreams(t *testing.T) {
	m, exec, _, _ := newFallbackFixture(t, "fb-stream")
	exec.replies["gpt-5.6-luna"] = quota429()
	res, err := m.ExecuteStream(context.Background(), []string{"fb-stream"}, cliproxyexecutor.Request{Model: "gpt-5.6-luna"}, cliproxyexecutor.Options{Stream: true})
	if err != nil {
		t.Fatalf("stream: %v (calls=%v)", err, exec.calls)
	}
	var payload string
	for chunk := range res.Chunks {
		if chunk.Err != nil {
			t.Fatalf("chunk err: %v", chunk.Err)
		}
		payload += string(chunk.Payload)
	}
	if payload != `{"model":"gpt-reserve"}` {
		t.Fatalf("expected reserve stream, got %s (calls=%v)", payload, exec.calls)
	}
	want := []string{"fb-stream-entitled/gpt-5.6-luna", "fb-stream-plain/gpt-5.6-luna", "fb-stream-entitled/gpt-reserve"}
	if len(exec.calls) != len(want) {
		t.Fatalf("calls=%v want=%v", exec.calls, want)
	}
	for i := range want {
		if exec.calls[i] != want[i] {
			t.Fatalf("call %d = %s, want %s (all=%v)", i, exec.calls[i], want[i], exec.calls)
		}
	}
}

// A non-quota failure must not trigger the fallback pass: a 400 repeated against the reserve
// would waste a scarcer allowance on a request that cannot succeed.
func TestFallbackPassSkippedOnRequestErrors(t *testing.T) {
	m, exec, _, _ := newFallbackFixture(t, "fb-400")
	exec.replies["gpt-5.6-luna"] = &Error{HTTPStatus: http.StatusBadRequest, Message: "invalid request"}
	if _, err := m.Execute(context.Background(), []string{"fb-400"}, cliproxyexecutor.Request{Model: "gpt-5.6-luna"}, cliproxyexecutor.Options{}); err == nil {
		t.Fatalf("expected the 400 to surface")
	}
	for _, call := range exec.calls {
		if call == "fb-400-entitled/gpt-reserve" {
			t.Fatalf("reserve must not be tried after a request error: %v", exec.calls)
		}
	}
}
