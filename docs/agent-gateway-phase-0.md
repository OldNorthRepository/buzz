# Agent gateway Phase 0: launch policy

New managed-agent create requests default `startOnAppLaunch` to `false`. An
operator must explicitly set it to `true` for a control-plane agent that is
intended to start when Desktop launches. This is fail-safe: no implicit
identity-based exception exists.

## Roster status

No authoritative core-control-agent roster is available in the repository or
this implementation Brief. Phase 0 therefore does not invent a roster,
provision identities, or modify persisted agent records. Existing records keep
their stored `start_on_app_launch` value; records created before that field
existed retain the historical deserialization default for compatibility.

An authoritative roster and its ownership process are required before a future
migration changes existing launch flags.

## Repository write routes

Gateway name-convention auto-routes remain read-only. Write-capable execution
requires an explicit immutable channel UUID mapping:

```sh
BUZZ_ACP_GATEWAY_WRITE_ROUTES='<channel-uuid>=<route-id>'
```

At startup, `buzz-acp` verifies that every referenced route exists, uses the
`repository_write` profile, and binds the same channel UUID in its gateway
contexts. Write turns submit the matched `repository_write` + `workspace_write`
contract; all other routes submit `repository_read` + `external_effects`.
The gateway host must independently opt in to workspace writes, canonicalizes
route paths below its configured workspace roots, and applies exclusive
per-workspace write scheduling. A display-name slug can never select the
declared write path. Permission profiles govern routing, scheduling, and
recovery; they do not provide an OS filesystem sandbox for the runtime process.
