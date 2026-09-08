#!/usr/bin/env bash

# 查询当前代理出口的公网地址；不要把 127.0.0.1、局域网地址或虚拟网卡地址加入白名单。
set -u

curl_bin="${CURL_BIN:-curl}"
timeout_seconds="${IP_QUERY_TIMEOUT_SECONDS:-5}"
mode="ipv4"
verbose="false"

for argument in "$@"; do
  case "$argument" in
    --ipv4) mode="ipv4" ;;
    --ipv6) mode="ipv6" ;;
    --verbose) verbose="true" ;;
    -h|--help)
      cat <<'USAGE'
用法：bash scripts/current_ip.sh [--ipv4|--ipv6] [--verbose]

默认查询当前 HTTPS 代理出口的公网 IPv4。输出的地址可用于币安 API
IP 白名单；请确认运行命令的网络代理就是实际交易进程使用的代理。
USAGE
      exit 0
      ;;
    *)
      printf '未知参数：%s\n' "$argument" >&2
      exit 2
      ;;
  esac
done

if ! command -v "$curl_bin" >/dev/null 2>&1; then
  printf '找不到 curl：%s\n' "$curl_bin" >&2
  exit 127
fi

case "$mode" in
  ipv4)
    curl_family=(-4)
    providers=(
      "https://api.ipify.org"
      "https://ifconfig.me/ip"
      "https://icanhazip.com"
    )
    ;;
  ipv6)
    curl_family=(-6)
    providers=(
      "https://api6.ipify.org"
      "https://ifconfig.me/ip"
      "https://icanhazip.com"
    )
    ;;
esac

is_valid_ipv4() {
  local value="$1"
  local part numeric_part
  local -a octets
  [[ "$value" =~ ^[0-9]{1,3}(\.[0-9]{1,3}){3}$ ]] || return 1
  IFS='.' read -r -a octets <<< "$value"
  for part in "${octets[@]}"; do
    [[ "$part" == 0 || "$part" != 0* ]] || return 1
    numeric_part=$((part))
    (( numeric_part <= 255 )) || return 1
  done
}

is_valid_address() {
  local value="$1"
  if [[ "$mode" == "ipv4" ]]; then
    is_valid_ipv4 "$value"
  else
    [[ "$value" == *:* && "$value" =~ ^[0-9A-Fa-f:.]+$ ]]
  fi
}

for provider in "${providers[@]}"; do
  address="$("$curl_bin" "${curl_family[@]}" --fail --silent --show-error \
    --location --max-time "$timeout_seconds" "$provider" 2>/dev/null || true)"
  address="${address//$'\r'/}"
  address="${address//$'\n'/}"
  address="${address//[[:space:]]/}"
  if is_valid_address "$address"; then
    if [[ "$verbose" == "true" ]]; then
      printf 'address=%s\nsource=%s\nprotocol=%s\n' "$address" "$provider" "$mode"
    else
      printf '%s\n' "$address"
    fi
    exit 0
  fi
done

printf '无法查询公网%s；请检查 curl、代理和网络连接。\n' "$mode" >&2
exit 1
