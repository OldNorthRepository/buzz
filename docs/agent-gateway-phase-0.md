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
