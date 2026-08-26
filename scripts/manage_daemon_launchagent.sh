#!/usr/bin/env bash
set -euo pipefail

usage() {
  printf 'Usage: %s install\n' "${0##*/}" >&2
}

if [[ "${1:-}" != "install" || "$#" -ne 1 ]]; then
  usage
  exit 64
fi

home_dir="${HOME:?HOME must be set}"
install_dir="${SNOW_RELEASE_INSTALL_DIR:-$home_dir/.cargo/bin}"
launch_agents_dir="${SNOW_DAEMON_LAUNCH_AGENTS_DIR:-$home_dir/Library/LaunchAgents}"
log_dir="${SNOW_DAEMON_LOG_DIR:-$home_dir/Library/Logs/snow}"
environment_name="${SNOW_DAEMON_ENV:-prd}"
daemon_path="${SNOW_DAEMON_PATH:-/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin}"
start_timeout_secs="${SNOW_DAEMON_START_TIMEOUT_SECS:-10}"
bootstrap_timeout_secs="${SNOW_DAEMON_BOOTSTRAP_TIMEOUT_SECS:-10}"
endpoint_timeout_secs="${SNOW_DAEMON_ENDPOINT_TIMEOUT_SECS:-45}"
owner_drain_timeout_secs="${SNOW_DAEMON_OWNER_DRAIN_TIMEOUT_SECS:-15}"
launchctl_bin="${SNOW_LAUNCHCTL_BIN:-launchctl}"
plutil_bin="${SNOW_PLUTIL_BIN:-plutil}"
ps_bin="${SNOW_DAEMON_PS_BIN:-ps}"
kill_bin="${SNOW_DAEMON_KILL_BIN:-/bin/kill}"
label="${SNOW_DAEMON_LAUNCH_LABEL:-com.servicenow-agent.snow-daemon}"
snow_bin="$install_dir/snow"
plist="$launch_agents_dir/$label.plist"
user_id="$(id -u)"
launch_domain="gui/$user_id"
service_target="$launch_domain/$label"

if [[ ! -x "$snow_bin" ]]; then
  printf 'snow executable not found at %s\n' "$snow_bin" >&2
  exit 1
fi

if ! [[ "$start_timeout_secs" =~ ^[0-9]+$ && "$bootstrap_timeout_secs" =~ ^[0-9]+$ && "$endpoint_timeout_secs" =~ ^[0-9]+$ && "$owner_drain_timeout_secs" =~ ^[0-9]+$ ]]; then
  printf 'SNOW_DAEMON_START_TIMEOUT_SECS, SNOW_DAEMON_BOOTSTRAP_TIMEOUT_SECS, SNOW_DAEMON_ENDPOINT_TIMEOUT_SECS, and SNOW_DAEMON_OWNER_DRAIN_TIMEOUT_SECS must be non-negative integers\n' >&2
  exit 64
fi

xml_escape() {
  local value="$1"
  value="${value//&/\&amp;}"
  value="${value//</\&lt;}"
  value="${value//>/\&gt;}"
  value="${value//\"/\&quot;}"
  value="${value//\'/\&apos;}"
  printf '%s' "$value"
}

drain_existing_daemon_owners() {
  local pid command deadline
  local owners=""
  while read -r pid command; do
    [[ "$pid" =~ ^[0-9]+$ ]] || continue
    case "$command" in
      "$snow_bin daemon __serve"*) owners+=" $pid" ;;
    esac
  done < <("$ps_bin" -axo pid=,command=)

  for pid in $owners; do
    "$kill_bin" -TERM "$pid" >/dev/null 2>&1 || true
  done

  deadline=$((SECONDS + owner_drain_timeout_secs))
  for pid in $owners; do
    while "$kill_bin" -0 "$pid" >/dev/null 2>&1; do
      if (( SECONDS >= deadline )); then
        printf 'existing daemon owner %s did not exit within %ss\n' \
          "$pid" "$owner_drain_timeout_secs" >&2
        exit 1
      fi
      sleep 1
    done
  done
}

mkdir -p "$launch_agents_dir" "$log_dir"
staged_plist="$(mktemp "$launch_agents_dir/.${label}.plist.XXXXXX")"

cat > "$staged_plist" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>$(xml_escape "$label")</string>
    <key>ProgramArguments</key>
    <array>
        <string>$(xml_escape "$snow_bin")</string>
        <string>daemon</string>
        <string>__serve</string>
        <string>--env</string>
        <string>$(xml_escape "$environment_name")</string>
        <string>--no-idle-timeout</string>
    </array>
    <key>EnvironmentVariables</key>
    <dict>
        <key>PATH</key>
        <string>$(xml_escape "$daemon_path")</string>
    </dict>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <dict>
        <key>SuccessfulExit</key>
        <false/>
    </dict>
    <key>ThrottleInterval</key>
    <integer>10</integer>
    <key>ProcessType</key>
    <string>Background</string>
    <key>StandardOutPath</key>
    <string>$(xml_escape "$log_dir/snow-daemon.stdout.log")</string>
    <key>StandardErrorPath</key>
    <string>$(xml_escape "$log_dir/snow-daemon.stderr.log")</string>
</dict>
</plist>
EOF

if ! "$plutil_bin" -lint "$staged_plist" >/dev/null; then
  rm -f "$staged_plist"
  printf 'generated LaunchAgent plist did not pass validation\n' >&2
  exit 1
fi
mv "$staged_plist" "$plist"

# The launchd-owned foreground process is the sole daemon owner. Do not invoke
# `snow daemon start` or `snow daemon restart` here: both create a detached
# child that can outlive launchd's lifecycle bookkeeping.
"$launchctl_bin" bootout "$service_target" >/dev/null 2>&1 || true
drain_existing_daemon_owners
bootstrap_deadline=$((SECONDS + bootstrap_timeout_secs))
until "$launchctl_bin" bootstrap "$launch_domain" "$plist"; do
  if (( SECONDS >= bootstrap_deadline )); then
    printf 'LaunchAgent %s could not be bootstrapped within %ss\n' \
      "$service_target" "$bootstrap_timeout_secs" >&2
    exit 1
  fi
  sleep 1
done
"$launchctl_bin" enable "$service_target"
"$launchctl_bin" kickstart -k "$service_target"

deadline=$((SECONDS + start_timeout_secs))
while :; do
  service_status="$("$launchctl_bin" print "$service_target" 2>&1 || true)"
  if [[ "$service_status" == *"state = running"* && "$service_status" == *"pid = "* ]]; then
    break
  fi
  if (( SECONDS >= deadline )); then
    printf 'LaunchAgent %s did not reach running state within %ss\n' \
      "$service_target" "$start_timeout_secs" >&2
    printf '%s\n' "$service_status" >&2
    exit 1
  fi
  sleep 1
done

endpoint_deadline=$((SECONDS + endpoint_timeout_secs))
while :; do
  daemon_status="$("$snow_bin" daemon status 2>&1 || true)"
  if [[ "$daemon_status" == "running" || "$daemon_status" == running$'\n'* ]]; then
    break
  fi
  if (( SECONDS >= endpoint_deadline )); then
    printf 'LaunchAgent %s is running but its daemon endpoint did not become reachable within %ss\n' \
      "$service_target" "$endpoint_timeout_secs" >&2
    printf '%s\n' "$daemon_status" >&2
    exit 1
  fi
  sleep 1
done

printf 'installed and started %s\n' "$service_target"
