#!/usr/bin/env bash
set -u

# 只执行 LIVE 的签名只读检查和 order/test，不创建真实订单。
python_bin="./.venv/Scripts/python.exe"
symbol="${BINANCE_SYMBOL:-BTCUSDT}"
spot_notional="${BINANCE_SPOT_TEST_NOTIONAL:-20}"
futures_quantity="${BINANCE_FUTURES_TEST_QUANTITY:-0.001}"
futures_position_side="${BINANCE_FUTURES_POSITION_SIDE:-}"
confirm="ENABLE LIVE TRADING"

run_check() {
  local name="$1"
  shift
  printf '\n=== %s ===\n' "$name"
  "$@"
}

run_check "Spot LIVE status" \
  "$python_bin" -m gribuki_trade binance-live-status \
  --symbol "$symbol" --confirm "$confirm"

run_check "Spot LIVE order test" \
  "$python_bin" -m gribuki_trade binance-live-order-test \
  --symbol "$symbol" --notional "$spot_notional" --confirm "$confirm"

run_check "USD-M Futures LIVE status" \
  "$python_bin" -m gribuki_trade binance-live-futures-status \
  --symbol "$symbol" --confirm "$confirm"

futures_order_test=(
  "$python_bin" -m gribuki_trade binance-live-futures-order-test
  --symbol "$symbol" --side BUY --quantity "$futures_quantity" --confirm "$confirm"
)
if [[ -n "$futures_position_side" ]]; then
  futures_order_test+=(--position-side "$futures_position_side")
fi
run_check "USD-M Futures LIVE order test" "${futures_order_test[@]}"
