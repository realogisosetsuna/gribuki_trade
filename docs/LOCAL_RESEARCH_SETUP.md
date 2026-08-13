# 本机 A 股研究集成配置

更新日期：2026-08-13

## 1. 首批研究池

`config/ashare_research_watchlist.toml` 当前包含 36 只股票和 9 只 ETF，覆盖沪深
主板、创业板、科创板，大/中/小规模研究层级，以及金融、消费、制造、科技、
医药、能源、资源、公用事业和交通运输。ETF 用于宽基、规模、成长和行业归因。

默认轮询子集为 18 个标的；显式传入一个或多个 `--symbol` 会覆盖默认池，
`--watchlist-all` 才会运行全部 45 个标的。研究池不是投资组合，也不能授权下单。

```powershell
.\.venv\Scripts\python.exe -m gribuki_trade ashare-watchlist
.\.venv\Scripts\python.exe -m gribuki_trade ashare-research-watch --cycles 1
.\.venv\Scripts\python.exe -m gribuki_trade ashare-research-watch --watchlist-all --cycles 1
```

## 2. DeepSeek V4 Flash

默认 provider 为 DeepSeek，默认模型 ID 为 `deepseek-v4-flash`。适配器使用官方
`https://api.deepseek.com/chat/completions`，只发送公开行情、公开新闻及其证据 ID；
不发送账户、订单、QQ 身份或个人资料。模型不能调用工具、通知或券商接口；返回
结果还要经过本地严格 schema 和证据引用校验，失败时退化为无宏观结论。
每次部署可用 `deepseek-status` 对官方 `GET /models` 做只读检查；输出的
`default_model_id` 和 `default_model_available` 明确表示当前默认模型是否可用。
官方文档说明 `deepseek-v4-flash` 与 `deepseek-v4-pro` 使用相同的思考强度映射，
因此 Flash 继续使用现有 `thinking`、`reasoning_effort` 和 JSON Output 请求格式。
DeepSeek 同时说明 JSON Output 偶尔可能返回空内容。适配器首次仍使用
`thinking=enabled` / `reasoning_effort=high`；仅当返回空内容、输出被截断、推理资源
中断或本地 schema 校验失败时，才执行一次 `thinking=disabled`、最长 60 秒的恢复
请求。恢复请求不会携带首次模型正文，也不会放宽 schema 或证据 ID 校验；内容过滤、
意外工具调用、未知证据引用不会重试。这样把最坏情况下的额外模型费用和等待限制为
一次请求。

安全录入入口：

```powershell
.\.venv\Scripts\python.exe -m gribuki_trade deepseek-configure
```

命令随后显示隐藏输入提示。在该提示中粘贴 API key 并回车；不要把 key 写进命令
参数、`.env`、文本文件或聊天。密钥存入 Windows 凭据管理器。配置后运行：

```powershell
.\.venv\Scripts\python.exe -m gribuki_trade deepseek-status
.\.venv\Scripts\python.exe -m gribuki_trade ashare-research-once --symbol 510300.SH --macro
```

如需暂时使用 OpenAI，可显式指定 `--macro-provider openai --model gpt-5.6`。DeepSeek
官方当前未文档化 `store:false`，Context Cache 默认启用，因此仍只允许公开证据进入
请求。

官方资料：

- [DeepSeek 模型与定价](https://api-docs.deepseek.com/quick_start/pricing)
- [DeepSeek 可用模型列表](https://api-docs.deepseek.com/api/list-models)
- [DeepSeek 思考模式](https://api-docs.deepseek.com/guides/thinking_mode)
- [Chat Completions](https://api-docs.deepseek.com/api/create-chat-completion)
- [JSON Output](https://api-docs.deepseek.com/guides/json_mode)
- [Context Cache](https://api-docs.deepseek.com/guides/kv_cache)

### 2.1 收盘深度分析（下一交易日）

盘中入口的分钟线最大新鲜度为 3 分钟，因此 15:00 后运行
`ashare-research-once` 得到 `STALE_MARKET_DATA` 是正确行为。收盘分析使用独立入口，
不会放宽盘中门禁：

```powershell
# 默认刷新新浪/财联社/东财/同花顺及标的资讯，并调用 deepseek-v4-flash
.\.venv\Scripts\python.exe -m gribuki_trade ashare-close-research-once `
  --symbol 510300.SH

# 只验证日历、日线和确定性技术指标，不抓新闻、不调用模型
.\.venv\Scripts\python.exe -m gribuki_trade ashare-close-research-once `
  --symbol 510300.SH --no-refresh-news --no-macro

# 已持仓时启用日线减仓触发器
.\.venv\Scripts\python.exe -m gribuki_trade ashare-close-research-once `
  --symbol 510300.SH --held
```

该入口使用 BaoStock 的真实交易日历，不按工作日猜测节假日。交易日 15:05 后以当天
为最近完成日；开盘前以之前交易日为完成日、当天为目标日；交易时段内直接返回
`MARKET_SESSION_NOT_CLOSED`。日线必须是未复权、严格有序、包含最近完成交易日；
停牌、缺日线、历史不足、除权断点或 provider 故障均 fail closed 为 `ABSTAIN`。

输出中的 `technical_metrics` 包含 MA5/20/60、20 日突破/支撑、量比、RSI14、
ATR14、5/20 日收益和 20 日波动率；`macro` 包含 DeepSeek 的证据化情景、主张、
不确定性和失效条件。每次实际使用的日线按 SHA-256 归档在
`runtime/research/market_evidence`，推荐及目标交易日保存在
`runtime/research/research.sqlite3`。模型只解释公开证据，不生成订单。

如需指定历史回放日期，可同时提供 `--session-date YYYY-MM-DD` 与
`--next-session YYYY-MM-DD`；二者仍会经过真实日历、相邻交易日和时点边界校验，
不能用参数绕过未来数据门禁。

## 3. NapCatQQ

本机已经准备：

- 官方源码：`vendor/NapCatQQ`，commit `fe7a1f053afd2473641f78a18dfac3b561167ff3`。
- 官方 v4.18.18 Shell：`vendor/NapCatQQ-shell-v4.18.18`；下载包 SHA-256 为
  `F1053918FAE7AE24807841BAA516D231F5412FC443FA217183698764BE1C1817`，
  已与 GitHub 官方 release metadata 匹配。
- Shell 使用 NapCat v4.18.18 官方构建脚本指定的 QQ 9.9.32.50969；安装包具有
  `Tencent Technology (Shenzhen) Company Limited` 的有效 Authenticode 签名，
  本机计算 SHA-256 为
  `8998862A704A9CF02E07615E653F631EF73FD8C82F05E26BE1A4A2A272C0781F`。
- 官方 `NapCat.Shell.Windows.Node.zip` 已确认存在上游打包缺陷：缺少
  `wrapper.node` 的 `crypto.dll`/`ssl.dll` 普通依赖，因此不作为活动运行时。
- OneBot：仅 `127.0.0.1:3000`，Bearer token，禁 CORS、WebSocket、HTTP client、
  事件回调和入站命令。
- WebUI：仅 `127.0.0.1:6099`，只允许 loopback，禁 X-Forwarded-For。
- 两枚随机 token 已写入 Windows 凭据管理器；NapCat 所需副本位于被 Git 忽略的
  runtime config 中。

启动命令：

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\scripts\start_napcat.ps1
```

启动脚本会在当前进程内把控制台统一为 UTF-8，并为旧版 Console Host 启用 ANSI
转义解析，避免中文、终端二维码以及 `[33m` 等颜色控制码乱码。该设置不会修改
Windows 区域选项或 PowerShell 全局配置。

脚本仅启动工作区内的腾讯签名 QQ 和官方 Shell，不安装系统级 QQ。首次启动会在
`vendor/NapCatQQ-shell-v4.18.18/cache/qrcode.png` 生成二维码；使用专用 QQ Bot
账号扫码，并在手机 QQ 完成设备确认。登录后可打开 `http://127.0.0.1:6099`。
不要把 QQ 密码、短信验证码、cookie、OneBot token 或 WebUI token 发给项目。

扫码成功后，回到项目目录执行：

```powershell
.\.venv\Scripts\python.exe -m gribuki_trade napcat-status
.\.venv\Scripts\python.exe -m gribuki_trade napcat-send-test `
  --target-kind private --target-id 你的目标QQ号 --confirm SEND_TEST
```

#### NapCat 登录与掉线恢复（Windows 11）

普通 QQ 窗口仍在并不代表 NapCat 正在运行。项目使用的是工作区内的注入版 QQ；
`napcat-status` 返回 `transport_error` 且 `3000`、`6099` 均未监听时，按下面顺序恢复：

1. 保存正在编辑的聊天内容。若 Bot 账号正由普通 QQ 登录，先从系统托盘完整退出该
   Bot 实例；其他 QQ 账号可以保留。
2. 在项目根目录单独打开一个 PowerShell 窗口并运行：

   ```powershell
   powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\scripts\start_napcat.ps1
   ```

3. 首次或登录态失效时，不传 QQ 号。等待 QQ 窗口或控制台二维码出现，用手机 QQ 中
   的专用 Bot 账号扫码，并在手机端确认新设备登录。启动窗口需要保持运行。
4. 打开 `http://127.0.0.1:6099/webui/`。WebUI token 只从启动控制台或本地
   `vendor/NapCatQQ-shell-v4.18.18/config/webui.json` 读取，不要粘贴到聊天或命令行。
5. 在 WebUI 的网络配置中核对 `gribuki-local` HTTP 服务端已经启用，地址为
   `127.0.0.1`、端口为 `3000`。不要启用公网监听、CORS、反向 WebSocket 或入站命令。
6. 检查两端口并再次运行状态命令：

   ```powershell
   Test-NetConnection 127.0.0.1 -Port 6099
   Test-NetConnection 127.0.0.1 -Port 3000
   .\.venv\Scripts\python.exe -m gribuki_trade napcat-status
   ```

   预期两个 `TcpTestSucceeded` 均为 `True`，状态结果中 `good` 和 `online` 均为
   `true`。服务未启动时命令会返回结构化的 `error_code=transport_error`，不会再打印
   traceback，并在 `next_action` 中直接给出本项目的启动命令。

首次扫码成功后可以用已登录过的 Bot 号快速启动：

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass `
  -File .\scripts\start_napcat.ps1 -QQAccount 你的BotQQ号
```

常见状态的含义：

- `6099` 与 `3000` 都关闭：启动的是普通 QQ、NapCat 启动窗口已退出，或注入程序被
  安全软件隔离。
- `6099` 开、`3000` 关：NapCat 已运行，但当前账号的 OneBot HTTP 服务未启用；在
  WebUI 检查 `onebot11_<当前Bot号>.json` 对应配置。
- `good=true`、`online=false`：NapCat 进程正常，但 QQ 会话离线；进入 WebUI 的
  “QQ 登录”页刷新二维码。
- `authentication_rejected`：OneBot token 与 Windows 凭据库不一致。使用
  `secret-set napcat.onebot.access_token` 在本机隐藏输入正确 token；不要误填 WebUI token。
- 端口被占用：以控制台显示的实际端口为准，并为 CLI 显式传 `--base-url`；不要把服务
  改为公网地址。

#### 图片、Markdown 与文件报告

NapCat/OneBot 可以发送图片，也支持 `upload_private_file` 和 `upload_group_file`。
`.md` 可以作为普通文件上传，但 QQ 客户端是否直接预览 Markdown、表格或公式取决于
接收端版本。NapCat 的原生 `markdown` 消息段不能可靠地像普通消息一样直发；官方兼容
说明将它限定在双层合并转发内，因此不作为生产默认方案。

报告采用三层降级设计：聊天内发送短文本摘要；把完整报告分页渲染为 PNG 供手机直接
阅读；同时上传 UTF-8 `.md` 文件供搜索、复制和电脑端查看。公式在 PNG 中由本地渲染器
排版，不依赖 QQ 客户端的 LaTeX 能力。任何附件只能来自项目的报告目录，不能由新闻或
模型文本指定任意本地路径或远程 URL。

`ashare-close-research-once` 默认把 Markdown 与 PNG 页写到 `runtime/reports`，并在
JSON 结果的 `report_markdown`、`report_images` 中返回精确路径。NapCat 恢复在线后，
可以逐个显式发送；下面的 `--artifact` 应使用相对于报告根目录的文件名：

```powershell
# 发送一页 PNG
.\.venv\Scripts\python.exe -m gribuki_trade napcat-send-artifact `
  --target-kind private --target-id 你的目标QQ号 `
  --artifact-kind image --artifact-root runtime/reports `
  --artifact 510300.SH-示例-page-01.png --confirm SEND_ARTIFACT

# 上传完整 Markdown 文件
.\.venv\Scripts\python.exe -m gribuki_trade napcat-send-artifact `
  --target-kind private --target-id 你的目标QQ号 `
  --artifact-kind file --artifact-root runtime/reports `
  --artifact 510300.SH-示例.md --confirm SEND_ARTIFACT
```

附件发送只允许报告根目录内的普通文件，拒绝 URL、UNC、路径穿越、符号链接、伪造扩展
和超限文件；私聊或群聊目标仍必须逐次进入精确白名单。

发送收盘报告采用“先持久入箱、再派发”的两步方式。`ABSTAIN` 也会作为状态报告
发送，但不会产生交易动作：

```powershell
.\.venv\Scripts\python.exe -m gribuki_trade ashare-close-research-once `
  --symbol 510300.SH `
  --notify-target-kind private `
  --notify-target-id 1320017950

.\.venv\Scripts\python.exe -m gribuki_trade napcat-dispatch `
  --target-kind private `
  --target-id 1320017950 `
  --cycles 1
```

群通知把 `private` 改成 `group`，并填写群号。Bot 必须已加入该群且能正常发言。
通知模块只做出站发送，目标 ID 进入精确白名单，成功消息经 SQLite outbox 去重。

需要用户完成或提供的仅有：

1. 在本机隐藏输入提示中录入 DeepSeek API key。
2. 选择一个不承载重要聊天、支付或资产的专用 QQ Bot 账号，并本人扫码登录。
3. 在本机运行通知命令时填入目标类型（`private`/`group`）以及目标 QQ 号或群号；
   无需在聊天里发送这些信息。
4. 决定后续是否需要 Windows 自动启动；初期建议手动启动。

NapCat 基于 NTQQ 的非官方框架，可能遇到设备验证、掉线或社交风控。官方来源：

- [NapCatQQ 仓库](https://github.com/NapNeko/NapCatQQ)
- [Windows Shell/便携部署](https://napneko.github.io/guide/boot/Shell)
- [OneBot 配置](https://napneko.github.io/config/basic)
- [安全说明](https://napneko.github.io/other/security)
