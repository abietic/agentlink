#!/bin/bash
# Installs the latest agentlink-server release when it is newer than the installed one, and
# rolls back if the new binary does not come up healthy. Touches only the agentlink unit.
# Run by examples/agentlink-update.timer; `update-server.sh --check` downloads and verifies the
# latest release without installing it.
#
# Besides the checksum, the binary must carry a GitHub artifact attestation from this repo's
# release workflow for that tag, checked with gh against a bundle from the public API, so no
# GitHub login is needed. A release that fails any check is not installed and the running
# server is left alone.
set -euo pipefail
REPO=${AGENTLINK_REPO:-abietic/agentlink}
DIR=${AGENTLINK_DIR:-/opt/agentlink}
UNIT=${AGENTLINK_UNIT:-agentlink-server}
HEALTH=${AGENTLINK_HEALTH:-http://127.0.0.1:8080/healthz}
GH=${AGENTLINK_GH:-$DIR/bin/gh}
check=false
[[ ${1:-} == --check ]] && check=true
[[ -x $GH ]] || { echo "$GH not found: cannot verify where a release was built" >&2; exit 1; }
case $(uname -m) in x86_64) arch=amd64 ;; aarch64) arch=arm64 ;; *) echo "unsupported arch" >&2; exit 1 ;; esac
asset=agentlink-server-linux-$arch

tag=$(curl -fsSL -o /dev/null -w '%{url_effective}' "https://github.com/$REPO/releases/latest")
tag=${tag##*/}
[[ $tag == v* ]] || { echo "no release found" >&2; exit 1; }
if ! $check && [[ $tag == "$(cat "$DIR/VERSION" 2>/dev/null)" || $tag == "$(cat "$DIR/FAILED" 2>/dev/null)" ]]; then
  exit 0
fi

tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
base=https://github.com/$REPO/releases/download/$tag
curl -fsSL -o "$tmp/$asset" "$base/$asset"
curl -fsSL -o "$tmp/SHA256SUMS" "$base/SHA256SUMS"
(cd "$tmp" && grep "  $asset\$" SHA256SUMS | sha256sum -c --quiet -)
digest=$(sha256sum "$tmp/$asset" | cut -d' ' -f1)
curl -fsSL "https://api.github.com/repos/$REPO/attestations/sha256:$digest" |
  python3 -I -c 'import json, sys; [print(json.dumps(a["bundle"])) for a in json.load(sys.stdin)["attestations"]]' >"$tmp/bundle.jsonl"
GH_CONFIG_DIR=$tmp/gh "$GH" attestation verify "$tmp/$asset" --repo "$REPO" --bundle "$tmp/bundle.jsonl" \
  --signer-workflow "$REPO/.github/workflows/release.yml" --source-ref "refs/tags/$tag" >/dev/null
if $check; then
  echo "$tag verified"
  exit 0
fi

install -m 755 "$tmp/$asset" "$DIR/agentlink-server.new"
cp -p "$DIR/agentlink-server" "$DIR/agentlink-server.prev"
mv "$DIR/agentlink-server.new" "$DIR/agentlink-server"
systemctl restart "$UNIT"
for _ in $(seq 10); do
  if curl -fsS --max-time 2 "$HEALTH" >/dev/null; then
    echo "$tag" > "$DIR/VERSION"
    echo "updated to $tag"
    exit 0
  fi
  sleep 2
done
echo "$tag did not come up healthy; rolled back, and will not retry this tag" >&2
mv "$DIR/agentlink-server.prev" "$DIR/agentlink-server"
systemctl restart "$UNIT"
echo "$tag" > "$DIR/FAILED"
exit 1
