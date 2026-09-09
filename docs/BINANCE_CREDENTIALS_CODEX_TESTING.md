# Binance 本机凭证：后续 Codex 验证流程

供后续会话复制执行。凭证只通过 CLI 验证，禁止读取、打印或复制密钥文件。

## 1. 凭证位置与用户

系统使用 Windows Keyring，并在 `%LOCALAPPDATA%\gribuki-trade\secrets.json` 保存同一用户的 DPAPI 加密副本。DPAPI 绑定 Windows 用户和机器，不能由 Codex 沙箱用户解密。

先确认 Git Bash 身份：

```bash
whoami
printf 'LOCALAPPDATA=%s\n' "$LOCALAPPDATA"
```

必须使用保存凭证的 Administrator 会话。若 `whoami` 是 `codexsandboxoffline`，`secret-status` 显示 `false` 属于用户隔离；不要重新录入或复制密文。

## 2. 只读验证

在仓库根目录执行：

```bash
./.venv/Scripts/python.exe -m gribuki_trade secret-status

./.venv/Scripts/python.exe -m gribuki_trade binance-live-status \
  --symbol BTCUSDT --confirm 'ENABLE LIVE TRADING'

./.venv/Scripts/python.exe -m gribuki_trade binance-live-futures-status \
  --symbol BTCUSDT --confirm 'ENABLE LIVE TRADING'

./.venv/Scripts/python.exe -m gribuki_trade binance-live-balance \
  --asset USDT --confirm 'ENABLE LIVE TRADING'

./.venv/Scripts/python.exe -m gribuki_trade binance-live-futures-balance \
  --asset USDT --confirm 'ENABLE LIVE TRADING'
```

成功标志：LIVE 凭证为 `true`；Spot 返回 `ping: ok`、`can_trade: true`；Futures 返回 `ping: ok`、`product: USDS_FUTURES`。余额命令只读账户。

## 3. 不创建订单的交易检查

`order-test` 只验证签名、权限和交易过滤器，不进入撮合、不创建订单：

```bash
./.venv/Scripts/python.exe -m gribuki_trade binance-live-order-test \
  --symbol BTCUSDT --notional 20 --confirm 'ENABLE LIVE TRADING'

./.venv/Scripts/python.exe -m gribuki_trade binance-live-futures-order-test \
  --symbol BTCUSDT --side BUY --position-side LONG --quantity 0.001 \
  --confirm 'ENABLE LIVE TRADING'
```

期望分别为 `entered_matching_engine: false` 和 `creates_order: false`。Futures Hedge Mode 必须指定 `LONG` 或 `SHORT`；One-way Mode 使用 `BOTH`。

## 4. 真实下单边界

只有用户明确授权才运行 `binance-live-order submit` 或
`binance-live-futures-order submit`。真实提交会改变远端账户；先确认余额、数量、价格、杠杆、方向、IP 白名单和 LIVE 确认短语。不要用真实下单测试凭证。

## 5. 凭证缺失时

仅在 Administrator 会话确认凭证确实不存在时重新录入：

```bash
./.venv/Scripts/python.exe -m gribuki_trade secret-set binance.live.api_key
./.venv/Scripts/python.exe -m gribuki_trade secret-set binance.live.secret_key
```

输入后关闭并重新打开同一用户终端，再执行第 2 节。任何报告只写布尔状态、余额、权限、时间偏移和测试结果，不得包含 API Key、Secret、签名、DPAPI 密文或请求头。
