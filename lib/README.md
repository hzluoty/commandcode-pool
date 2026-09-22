# Internal architecture boundaries

- `pool-store.js`: backend-neutral account-health persistence and one-account selection contract.
- `upstream-client.js`: transparent, single upstream generation transport; it never fabricates device/CLI identity or retries.
- `api-adapters.js`: Chat Completions, Messages, and Responses request adaptation boundary.
- `failure-policy.js`: client-visible error mapping (HTTP status to standard error shapes, Retry-After parsing) and explicit account-health intent.
- `generation-runner.js`: one logical request → one selected account → at most one upstream generation attempt.

The Worker entrypoint owns HTTP authentication, routing, and administration. New routing or account-health behavior must be added behind these boundaries rather than as another branch in `worker.js`.
