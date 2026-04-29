#!/usr/bin/env bash
set -euo pipefail

# Lightweight acceptance helper for real Android devices.
# No heavy build steps by default.

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
APP_DIR="$ROOT_DIR/mobile_app"
ADB_BIN="${ADB_BIN:-adb}"

info() { echo "[INFO] $*"; }
warn() { echo "[WARN] $*"; }
err()  { echo "[ERROR] $*" >&2; }

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    err "Missing required command: $1"
    exit 1
  }
}

check_device() {
  local count
  count="$("$ADB_BIN" devices | awk 'NR>1 && $2=="device" {print $1}' | wc -l | tr -d ' ')"
  if [[ "$count" -lt 1 ]]; then
    err "No authorized Android device found via adb."
    err "Connect device + enable USB debugging."
    exit 1
  fi
  info "Device detected."
}

install_debug_if_needed() {
  if [[ "${SKIP_INSTALL:-1}" == "1" ]]; then
    warn "Skipping install (SKIP_INSTALL=1)."
    warn "To install debug APK once, run:"
    warn "  cd \"$APP_DIR\" && flutter install -d android"
    return
  fi
  info "Installing debug build (may take time)..."
  (cd "$APP_DIR" && flutter install -d android)
}

start_and_collect_logs() {
  local pkg="com.example.mobile_app"
  local activity=".MainActivity"
  local out_dir="$ROOT_DIR/artifacts"
  local out_file="$out_dir/mobile_acceptance_logcat.txt"

  mkdir -p "$out_dir"

  info "Clearing old logcat buffer..."
  "$ADB_BIN" logcat -c || true

  info "Launching app..."
  "$ADB_BIN" shell am start -n "${pkg}/${activity}" >/dev/null
  sleep 8

  info "Collecting fresh app logs..."
  "$ADB_BIN" logcat -d | awk 'BEGIN{IGNORECASE=1} /Auth|login|Dio|gps|check|map|error|exception|MainActivity/ {print}' > "$out_file" || true
  info "Saved log snapshot: $out_file"
}

print_manual_test_plan() {
  cat <<'EOF'

==================== Manual Acceptance Steps ====================
1) Login
   - Open app -> login with valid user.
   - Expected: no generic "فشل تسجيل الدخول", clear error reason if failed.

2) التسجيل / التفريغ
   - Start recording 10-15 sec.
   - Tap GPS button once during recording.
   - Stop and send.
   - Expected: queue item sent, backend accepts gps_data, row appears in table.

3) الفرز (Stored Large + GPS)
   - Upload large file once (imports to Postgres).
   - Enable "استخدام البيانات المخزنة (Postgres)".
   - Upload small file, detect headers, run match.
   - Update my location, run GPS.
   - Expected: results + nearest plate + map link button works.

4) الخرائط
   - Press "موقعي الآن".
   - Upload GPS Excel.
   - Expected: points parsed from backend, pins shown on map, table preview visible.

5) التشيك
   - Connect, upload reference file, set plate column.
   - Try manual plate check.
   - Expected: live status updates and session rows behave correctly.

6) Admin (if admin user)
   - Open Admin tab.
   - Expected: page loads without auth/session break.
=================================================================

EOF
}

main() {
  require_cmd "$ADB_BIN"
  check_device
  install_debug_if_needed
  start_and_collect_logs
  print_manual_test_plan
  info "Done."
}

main "$@"
