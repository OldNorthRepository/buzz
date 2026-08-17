# Agent gateway Phase 0

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
