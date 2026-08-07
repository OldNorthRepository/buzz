#!/usr/bin/env bash
# Install a pre-built Buzz ACP release for the boiler-room-codex harness.
#
# This deliberately never reads or sources the service EnvironmentFile: it
# contains identity material managed by render-key.  The only configuration it
# installs is the non-secret channel denylist in the checked-in drop-in.
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: install-boiler-room-codex-denylist.sh --binary <release-buzz-acp> --release <id> [--dry-run]

Installs an immutable libexec release and atomically switches the
gateway-denylist/current symlink before restarting buzz-boiler-room-codex.
EOF
}

binary=''
release=''
dry_run=false
while (($#)); do
  case "$1" in
    --binary) binary=${2:?missing binary path}; shift 2 ;;
    --release) release=${2:?missing release id}; shift 2 ;;
    --dry-run) dry_run=true; shift ;;
    -h|--help) usage; exit 0 ;;
    *) printf 'unknown argument: %s\n' "$1" >&2; usage >&2; exit 2 ;;
  esac
done

[[ -n "$binary" && -n "$release" ]] || { usage >&2; exit 2; }
[[ "$release" =~ ^[0-9a-f]{7,64}$ ]] || { printf 'release must be a git SHA\n' >&2; exit 2; }
[[ -x "$binary" ]] || { printf 'release binary is not executable: %s\n' "$binary" >&2; exit 2; }

unit_dir="$HOME/.config/systemd/user/buzz-boiler-room-codex.service.d"
libexec_root="$HOME/.local/libexec/buzz-acp/gateway-denylist"
release_dir="$libexec_root/releases/$release"
dropin_source="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)/systemd/buzz-boiler-room-codex-denylist.conf"
dropin_target="$unit_dir/60-buzz-actions-denylist.conf"

if "$dry_run"; then
  printf 'would install %s to %s, switch %s/current, and restart buzz-boiler-room-codex.service\n' \
    "$binary" "$release_dir/buzz-acp" "$libexec_root"
  exit 0
fi

install -d -m 0755 "$release_dir" "$unit_dir"
install -m 0755 "$binary" "$release_dir/buzz-acp"
install -m 0644 "$dropin_source" "$dropin_target"
ln -sfn "releases/$release" "$libexec_root/current.new"
mv -Tf "$libexec_root/current.new" "$libexec_root/current"

systemctl --user daemon-reload
systemctl --user restart buzz-boiler-room-codex.service
systemctl --user is-active --quiet buzz-boiler-room-codex.service
