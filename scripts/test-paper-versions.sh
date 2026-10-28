#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Leoon-X
#
# Starts each given Paper and Folia version with the LeonsDB plugin, one at
# a time, and checks which way the plugin saves players for server switches:
# Paper must use its idle-time task queue, Folia the scheduler. Each server
# must start without LeonsDB errors and is stopped cleanly afterwards. No
# LeonsDB server is needed: the plugin starts without one and says so.
#
#   scripts/test-paper-versions.sh <LeonsDB-Paper.jar> paper:1.21.11 folia:1.21.11 ...
#
# Needs java (21 or newer, as the versions require), python3 and about 1 GB
# of memory. Servers listen on 127.0.0.1 only. Exits non-zero if any
# version fails.
set -uo pipefail

jar=$(realpath "$1"); shift
java=${JAVA:-java}
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
failed=0

download() { # project version out
  python3 - "$1" "$2" "$3" <<'PY'
import hashlib, json, sys, urllib.request
project, version, out = sys.argv[1:4]
headers = {"User-Agent": "leonsdb-version-test"}
url = f"https://fill.papermc.io/v3/projects/{project}/versions/{version}/builds"
build = json.load(urllib.request.urlopen(urllib.request.Request(url, headers=headers)))[0]
dl = build["downloads"]["server:default"]
data = urllib.request.urlopen(urllib.request.Request(dl["url"], headers=headers)).read()
if hashlib.sha256(data).hexdigest() != dl["checksums"]["sha256"]:
    sys.exit(f"{project} {version}: checksum mismatch")
open(out, "wb").write(data)
print(build["id"])
PY
}

for target in "$@"; do
  project=${target%%:*} version=${target#*:}
  dir=$work/$project-$version
  mkdir -p "$dir/plugins/LeonsDB"
  if ! build=$(download "$project" "$version" "$dir/server.jar"); then
    echo "FAIL $project $version: download failed"; failed=1; continue
  fi
  cp "$jar" "$dir/plugins/"
  echo "eula=true" > "$dir/eula.txt"
  printf 'server-ip=127.0.0.1\nserver-port=25699\nonline-mode=false\nwhite-list=true\nlevel-type=minecraft\\:flat\ngenerate-structures=false\nview-distance=2\nsimulation-distance=2\nspawn-monsters=false\n' > "$dir/server.properties"
  # Any token: the plugin needs one to start, and no server is reachable.
  printf 'server:\n  host: 127.0.0.1\n  port: 1\n  token: "version-test"\n' > "$dir/plugins/LeonsDB/config.yml"

  mkfifo "$dir/console"
  (cd "$dir" && exec "$java" -Xms256m -Xmx512m -XX:+UseSerialGC -XX:TieredStopAtLevel=1 \
      -jar server.jar --nogui < console > out.log 2>&1) &
  pid=$!
  exec 3> "$dir/console" # keeps the console open until we send "stop"

  for _ in $(seq 1 300); do
    grep -q "Done (" "$dir/out.log" && grep -q "Player data is saved for server switches" "$dir/out.log" && break
    kill -0 $pid 2>/dev/null || break
    sleep 1
  done
  echo stop >&3
  for _ in $(seq 1 60); do kill -0 $pid 2>/dev/null || break; sleep 1; done
  kill -0 $pid 2>/dev/null && { kill $pid; wait $pid 2>/dev/null; }
  exec 3>&-

  line=$(grep -o "Player data is saved for server switches.*" "$dir/out.log" | head -1)
  # Any trouble with LeonsDB, except that its server is not there.
  errors=$(grep "LeonsDB" "$dir/out.log" | grep -E "ERROR|SEVERE|Exception|Error" \
      | grep -vE "Could not reach LeonsDB|UNAVAILABLE" | head -3)
  case $project in
    paper) want="Paper's idle-time task queue" ;;
    *) want="(scheduler)" ;;
  esac
  if ! grep -q "Done (" "$dir/out.log"; then
    echo "FAIL $project $version build $build: the server did not start"; failed=1
  elif [[ $line != *"$want"* ]]; then
    echo "FAIL $project $version build $build: ${line:-no line about server switches}"; failed=1
  elif [ -n "$errors" ]; then
    echo "FAIL $project $version build $build: $errors"; failed=1
  elif ! grep -q "Saving all player data" "$dir/out.log"; then
    echo "FAIL $project $version build $build: the plugin was not disabled cleanly"; failed=1
  else
    echo "ok   $project $version build $build: $line"
  fi
  [ -n "${KEEP_LOGS:-}" ] && cp "$dir/out.log" "$KEEP_LOGS/$project-$version.log"
  rm -rf "$dir"
done
exit $failed
