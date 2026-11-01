#!/bin/bash
# Installs the latest agentlink-worker release (binary and SDK runner) on macOS once the
# worker is idle, after `gh attestation verify` confirms each file was built by this repo's
# release workflow. Run hourly by examples/com.agentlink.update.plist; needs a logged-in gh.
set -euo pipefail
REPO=${AGENTLINK_REPO:-abietic/agentlink}
HOME_DIR=${AGENTLINK_HOME:-$HOME/.agentlink}
LABEL=${AGENTLINK_LABEL:-com.agentlink.worker} # also restarts every loaded $LABEL.* (one per extra worker config)
case $(uname -m) in arm64) arch=arm64 ;; x86_64) arch=amd64 ;; *) echo "unsupported arch" >&2; exit 1 ;; esac
asset=agentlink-worker-darwin-$arch

tag=$(gh release view --repo "$REPO" --json tagName --jq .tagName)
[[ $tag == "$(cat "$HOME_DIR/VERSION" 2>/dev/null)" ]] && exit 0
for pid in $(pgrep -f "$HOME_DIR/bin/agentlink-worker" || true); do
  if pgrep -P "$pid" -f 'runner\.mjs|ui-cli' >/dev/null; then
    echo "$tag is out, but a run is in progress; trying again later"
    exit 0
  fi
done

tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
gh release download "$tag" --repo "$REPO" --dir "$tmp" -p "$asset" -p agentlink-sdk.tar.gz -p SHA256SUMS
(cd "$tmp" && shasum -a 256 -c --ignore-missing --quiet SHA256SUMS)
for f in "$asset" agentlink-sdk.tar.gz; do
  gh attestation verify "$tmp/$f" --repo "$REPO" --signer-workflow "$REPO/.github/workflows/release.yml" --source-ref "refs/tags/$tag" >/dev/null
done
mkdir "$tmp/sdk"
tar -xzf "$tmp/agentlink-sdk.tar.gz" -C "$tmp/sdk" --strip-components 1
(cd "$tmp/sdk" && npm ci --omit=dev --no-audit --no-fund --loglevel=error)

mkdir -p "$HOME_DIR/bin"
install -m 755 "$tmp/$asset" "$HOME_DIR/bin/agentlink-worker.new"
rm -rf "$HOME_DIR/sdk.prev"
if [[ -d $HOME_DIR/sdk ]]; then mv "$HOME_DIR/sdk" "$HOME_DIR/sdk.prev"; fi
mv "$tmp/sdk" "$HOME_DIR/sdk"
mv "$HOME_DIR/bin/agentlink-worker.new" "$HOME_DIR/bin/agentlink-worker"
echo "$tag" > "$HOME_DIR/VERSION"
for label in $(launchctl list | awk -v l="$LABEL" '$3 == l || index($3, l ".") == 1 {print $3}'); do
  launchctl kickstart -k "gui/$(id -u)/$label"
done
echo "updated to $tag"
