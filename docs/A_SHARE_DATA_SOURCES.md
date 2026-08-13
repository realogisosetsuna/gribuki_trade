# A 股研究系统数据源矩阵与分时装配方案

更新日期：2026-08-13（Asia/Shanghai）

本文定义 A 股收盘研究、次日盘前研究所使用的数据语义、优先级、可用时间和降级规则。它不是“能抓到什么就填什么”的接口清单，而是证据准入规范：不同定义的指标不得静默互换，缺失值不得写成 `0.000`，网页上的数据日期也不得直接当作实际发布时间。

## 1. 优先级和状态约定

### 1.1 数据源优先级

| 等级 | 定义 | 可否进入决策评分 | 典型用途 |
| --- | --- | --- | --- |
| P0-O：官方免费 | 官方机构、交易所或基准管理人的公开页面/文件；当前无需付费数据合同即可读取 | 可以，但必须通过 schema、日期、时区、新鲜度和 PIT 检查 | 官方日行情、利率定盘、收益率曲线、公告 |
| P0-S：二手降级 | AKShare、BaoStock 或公开资讯商接口；可能转引官方数据，也可能是供应商自有口径 | 可以降权进入，前提是语义明确、时间戳可信、质量检查通过；报告必须标“二手/降级” | 官方接口暂时不可用时的行情快照、ETF 供应商指标 |
| P1-L：付费许可 | 精确数据存在，但自动化、实时、历史回放或再分发需要数据许可/订阅 | 获得许可后可以；未获许可时只登记缺失 | ICE DXY、FTSE/CC CRB、Baltic BDI、交易所实时行情 |
| P2-R：仅展示/研究 | 无法稳定确认定义、时点、历史可复现性或来源链 | 不进入评分、回测、风控或下单；最多作为待核线索 | 未核验社媒、聚合榜单、旧北向资金接口、未定义“主力资金” |

“官方免费”只表示当前可公开读取，并不自动授予商业再分发、批量下载或衍生产品许可。系统当前按个人单机研究设计；任何远程服务或对外分发上线前，需要重新核对各来源条款。

### 1.2 实现状态

| 状态 | 含义 |
| --- | --- |
| 已实现 | 已有适配器、字段校验、时间/新鲜度元数据、失败隔离和测试，并已装配或可装配为收盘证据 |
| 进行中 | 已有部分端口、适配器或特征槽位，但报告编排、缓存、历史归档或生产实测尚未全部完成 |
| 待实现 | 已确认来源和语义，尚未进入代码 |
| 不可获得 | 现行公开披露制度下没有所需粒度；不得用旧接口或相似指标伪造 |

所有时间如无特别说明均为北京时间（Asia/Shanghai）。美国市场数据必须保留 `America/New_York` 原始时区并按夏令时转换，不能固定减 12 或 13 小时。

## 2. 决策级数据矩阵

### 2.1 A 股、ETF、指数和市场广度

| 数据项 | 精确语义 | 首选与降级来源 | 发布时间/时点 | 当前状态 | 失败与回退规则 |
| --- | --- | --- | --- | --- | --- |
| 股票/ETF 日线 | 交易所完成交易日的 OHLC、成交量、成交额；复权口径必须单列 | P1-L：交易所许可行情；P0-S：BaoStock 与 AKShare；第三级为此前内容寻址、不可变的本地已审计快照 | A 股连续竞价 15:00 结束；公开二手接口的实际可见时间以首次成功抓取为准 | **已实现：BaoStock→AKShare、受控尾拼接及 exact-session 本地快照回退** | 不得把前复权与不复权序列拼接；本地快照只在证券、来源、末交易日、SHA、schema、窗口和 PIT 全部精确匹配时使用，绝不把旧交易日冒充最新日 |
| 分钟/逐笔/盘口 | 明确 bar 周期、撮合时间、成交/委托属性的盘中数据 | P1-L：交易所或券商授权 Level-1/Level-2；P0-S：AKShare 公开站点包装 | 盘中；免费接口无稳定 SLA | 进行中/研究用途 | 免费“tick”常是快照或成交明细，不等于交易所全量逐笔；不能据此承诺低延迟实盘 |
| ETF 盘中上下文 | 最新价、IOPV、折溢价、换手率、成交额、份额、买一/卖一 | P0-S：AKShare `fund_etf_spot_em`（东方财富）；P0-O 对账：[上交所 ETF 市场数据](https://etf.sse.com.cn/marketdata/) 与 [上交所对外公示目录](https://www.sse.com.cn/market/publicdata/) | 供应商快照为盘中/盘后；交易所盘后数据没有统一承诺时刻，记录首次可见时间 | **已实现：ETF context；上交所结算后总份额已独立接入** | 必须精确匹配代码；无精确行则缺失。IOPV 是盘中参考净值，不是基金 NAV；官方单日总份额也不是申赎净流量 |
| ETF “主力净流入” | 资讯商按订单/成交大小划分的供应商指标 | P0-S/P2-R：东方财富供应商字段 | 随供应商快照 | 已采集，但只作供应商上下文 | **不得称为 ETF 申购赎回、机构资金或真实资金净流入**；不得与份额变化相加；默认低权重或仅展示 |
| ETF 官方规模/申赎 | 交易所/基金公司披露的规模、份额或 PCF；与二级市场成交资金不同 | P0-O：[上交所 ETF 市场数据](https://etf.sse.com.cn/marketdata/)、基金公司 PCF/公告；深交所基金数据 | 盘后，具体页面无统一 SLA 时用首次可见时间 | **已实现上交所官方总份额及真实统计日；PCF、NAV、深交所份额仍待实现** | 当日尚未发布时只允许回退到明确标注的最新实际统计日；不把份额水平、成交额或供应商资金字段称为净申购 |
| 沪深 300 指数 | 中证官方 000300 指数收盘值及收益 | P0-O：[中证指数官网](https://www.csindex.com.cn/) 和 [沪深 300 编制方案](https://oss-ch.csindex.com.cn/static/html/csindex/public/uploads/indices/detail/files/zh_CN/000300_Index_Methodology_cn.pdf)；P0-S 公开行情接口 | 指数实时发布，收盘后冻结当日值 | 基础指数已接入二手快照；官方对账待实现 | IF 基差只在“同一交易日、同一收盘定义”的指数现货存在时计算 |
| 指数成份与行业映射 | 在某个历史时点生效的成份、权重、行业分类 | P0-O：中证当前成份与调样公告；P1-L：带历史生效日的成份库 | 调样公告与生效日；不能用当前名单回填历史 | 进行中 | `index_stock_cons_csindex` 等当前名单不等于历史 PIT 名单；从现在开始按生效日归档，历史回测无快照则标缺失 |
| 全市场广度 | 同一时点全 A 股上涨/下跌/平盘、成交额分布；涨跌停和站上均线比例需额外规则/历史 | P0-S：东财全市场快照，失败后严格回退腾讯全市场快照；P1-L：交易所许可全市场行情 | 15:00 后首个完整快照；需要固定证券池和停牌处理 | **已实现收盘快照：东财→腾讯双源；沪深京齐备且有效样本不少于 4500 才采用** | 重复代码、缺任一交易所或样本门槛不足时整源失败；当前不伪造涨跌停家数；历史 PIT 归档及“站上 MA20 比例”仍待实现 |

### 2.2 国内流动性、利率和人民币

| 数据项 | 精确语义 | 首选与降级来源 | 发布时间/时点 | 当前状态 | 失败与回退规则 |
| --- | --- | --- | --- | --- | --- |
| Shibor | 高信用等级报价行的无担保人民币同业拆出报价，经剔除高低报价后算术平均；不是成交加权回购利率 | P0-O：[Shibor 官方页](https://www.shibor.org/chinese/llshibor/) | 每个交易日 11:00 | **已实现：官方八期限 O/N 至 1Y、日期与 PIT 校验** | 可保留上一交易日并标 stale；不得用 FR/FDR/DR 替代；官方站点的兼容 TLS 仅对白名单主机开启且仍校验证书和主机名 |
| FR001/FR007/FR014 | 9:00–11:30 银行间质押式回购对应期限全部合格成交利率排序后的中位定盘值 | P0-O：[中国货币网回购定盘利率](https://www.chinamoney.com.cn/chinese/bkfrr/)；P0-S：AKShare `repo_rate_query` 转取 | 每个交易日 11:30 起 | **已实现：liquidity context（FR 独立失败域）** | 报告必须写“FR007 回购定盘利率”；不能简称 DR007 |
| FDR001/FDR007/FDR014 | 存款类机构之间、以利率债为质押的回购样本中位定盘值 | P0-O：[中国货币网回购定盘利率](https://www.chinamoney.com.cn/chinese/bkfrr/)；P0-S：AKShare `repo_rate_query` 转取 | 每个交易日 11:30 起 | **已实现：liquidity context（FDR 独立失败域）** | 报告必须写“FDR007 银银间回购定盘利率”；FDR007 仍不是逐笔/成交量加权的 DR007 |
| DR007 | 存款类机构以利率债质押的 7 天回购实际成交加权利率（需采用明确的官方成交统计口径） | P0-O：[中国货币网货币市场行情](https://www.chinamoney.com.cn/chinese/mkdatapm/)；如自动历史接口受限则使用 P1-L 官方数据服务；不能由 FDR007 反推 | 日终值通常在银行间市场收盘后形成；以官方值首次可见时间为准 | 待实现 | 未取得精确源时保持缺失；绝不把 FDR007 改名为 DR007 |
| 中债国债收益率曲线 | 中债编制的在岸人民币国债到期收益率曲线，期限和曲线类型必须精确 | P0-O：[中债国债及其他债券收益率曲线](https://yield.chinabond.com.cn/cbweb-pbc-web/pbc/more?locale=cn_zh)；P0-S：AKShare `bond_china_yield` 转取 | 每个工作日 17:30 | **已实现：liquidity context（国债曲线独立失败域）** | 15:10/16:20 报告只能使用 T-1 最新已发布曲线；17:40 后才允许纳入 T 日曲线。输出 1Y/10Y/30Y 及 10Y−1Y、30Y−10Y，不混用财政部曲线或信用债曲线 |
| 人民币汇率中间价 | 中国外汇交易中心受权发布的当日人民币对各币种中间价；不是即期成交价或收盘价 | P0-O：[SAFE 人民币汇率中间价](https://www.safe.gov.cn/safe/rmbhlzjj/) 与 [9:15 发布规则](https://www.safe.gov.cn/safe/2014/0702/5725.html) | 每个工作日 09:15；有效至下一次中间价发布 | **已实现：保留“100美元兑人民币”原始口径并标准化为 CNY/USD** | 当日 09:15 前只能使用上一工作日中间价并标日期；不得把离岸 USD/CNH 或在岸收盘价称为中间价 |
| 在岸人民币即期收盘 | 银行间 USD/CNY 即期 16:30 收盘价，与 09:15 中间价含义不同 | P0-O/中国外汇交易中心公开数据；P1-L 实时/历史接口 | 官方说明 16:30 发布时间不变，见 [PBOC/SAFE 2022 第 17 号公告](https://www.safe.gov.cn/safe/2022/1230/22197.html) | 待实现 | 若只取得供应商实时价，标“供应商即期快照”，不得写“官方收盘价” |

FR007、FDR007 和 DR007 的关系必须在模型提示词与报告模板中保持：`FR007 != FDR007 != DR007`。前两者是定盘指标，后者通常指实际成交加权利率；即使数值恰好相同，也不能合并字段。

### 2.3 衍生品、杠杆和资金结构

| 数据项 | 精确语义 | 首选与降级来源 | 发布时间/时点 | 当前状态 | 失败与回退规则 |
| --- | --- | --- | --- | --- | --- |
| IF 日行情 | 中金所 IF 各上市合约的 OHLC、结算价、成交量和持仓量 | P0-O：[中金所日行情数据](https://www.cffex.com.cn/fzjy/mrhq/) 与 [IF 合约说明](https://www.cffex.com.cn/cn/hs300.html)；AKShare `futures_hist_daily_cffex` 作为读取层 | IF 15:00 收盘；日文件无明确 SLA 时记录首次成功获取时间 | **已实现：IF context** | 每个合约单独保留；主力合约规则需可审计。日文件未出现 T 日时不拿 T-1 冒充 |
| IF 基差 | 同一交易日 IF 合约收盘价减沪深 300 现货收盘；年化基差还需准确剩余期限 | P0-O/P0-S 的 IF 与 CSI300 同时存在后派生 | 两条输入均可用后 | **已实现条件计算；现货缺失即不计算** | 不跨交易日配对，不用 ETF 价格替代指数点位；临近换月同时展示近月/次月，避免主力切换假跳变 |
| 上交所 ETF 期权 Greeks | 交易所按当日收盘数据计算的合约级 Delta、Theta、Gamma、Vega、Rho | P0-O：[上交所期权风险指标](https://www.sse.com.cn/assortment/options/risk/) | 盘后；官网只说明按当日收盘计算，未承诺固定发布时刻 | **已实现：逐合约 Greeks/官方 IV、真实交易日、官方零值保留** | 精确日没有数据时可读取明确标注真实日期的 latest 文档，但不得冒充请求日；单纯合约计数不生成方向评分 |
| 510300 期权隐含波动率/偏度/期限结构 | 从上交所合约级隐含波动率、行权价、到期日和标的价格按公开算法派生的 ATM IV、skew、term structure | P0-O 上交所合约/风险数据；必要时 P1-L 期权行情 | 盘后，输入齐备后 | **底层官方 IV 已实现；ATM 选约、偏度与期限结构仍待实现** | 当前报告只展示合约覆盖与正值/官方零值计数，不做全合约简单平均；后续必须记录到期日、行权价、流动性筛选和插值规则 |
| QVIX | 国内资讯/研究者基于 ETF 期权构造的波动率序列，口径可能因提供方不同 | 只有在提供方给出完整方法、历史修订和时点后才能列 P0-S；否则 P2-R | 依提供方 | 未接入 | **QVIX 不是 Cboe VIX**，也不是上交所官方单一风险指标；不得用 QVIX 填充 `CBOE_VIX` |
| 融资融券 | 交易所汇总/明细的融资余额、融资买入额、融券余量等；不是“主力资金” | P0-O：[上交所融资融券明细与汇总](https://www.sse.com.cn/market/othersdata/margin/detail/index.shtml)、[深交所融资融券](https://www.szse.cn/www/marketServices/deal/finance/) | 盘后；页面无固定 SLA 时用首次可见时间 | 待实现 | 沪深口径先分别校验再合并；融券单位随证券类型变化；T 日未发布则保留 T-1 且明确日期 |
| 北向每日净买入/净流入 | 北向买入金额减卖出金额的逐日方向性净额 | 现行公开披露中不存在所需全市场日度买卖拆分 | 自 2024-08-19 起结构性不可获得 | **不可获得** | 不调用停止更新的旧东方财富/聚合接口，不用成交总额推算净额，不用托管持仓季度变化伪装日流量 |
| 北向仍可获得信息 | 收市后当日交易总额/笔数、ETF 交易总额、成交额前十证券；季度末持仓在随后第五个北向交易日披露 | P0-O：[深交所披露机制调整通知](https://www.szse.cn/szhk/hkbussiness/news/t20240726_608353.html)、[HKEX 联合调整说明](https://www.hkex.com.hk/News/Market-Communications/2024/2404122news?sc_lang=en) | 日度总额收市后；单股合计持有数量按季度披露 | 待实现为结构/活跃度证据 | 只能按原字段命名；交易总额只说明活跃度，不说明净方向。HKEX 也确认其余调整于 2024-08-19 实施，见 [实施通告](https://www.hkex.com.hk/-/media/HKEX-Market/Services/Circulars-and-Notices/Participant-and-Members-Circulars/HKSCC/2024/ce_HKSCC_NOM_217_2024.pdf) |

### 2.4 全球风险、外汇、商品和航运

| 数据项 | 精确语义 | 首选与降级来源 | 发布时间/时点 | 当前状态 | 失败与回退规则 |
| --- | --- | --- | --- | --- | --- |
| Cboe VIX | 由 SPX 期权报价计算、代表美国股票市场未来约 30 日预期波动的 Cboe 指数；不是可直接投资的价格 | P0-O：[Cboe VIX 历史日线](https://www.cboe.com/tradable_products/vix/vix_historical_data)，官方 CSV 每日更新；实时指数为 P1-L [Cboe Global Indices Feed](https://www.cboe.com/us/indices/accessing-index-data/) | 正常交易日观测收盘约 16:15 America/New_York；官方历史页仅承诺 daily update，不承诺文件刷新秒点 | **已实现并接入收盘报告：官方 CSV、严格 schema、PIT 过滤、缓存元数据与独立失败码** | A 股 15:10 只能看到最近已完成的美国交易日；官方当前历史文件含少量旧 OHLC 包络异常，适配器保留原始文件、隔离旧坏行并报告计数，若最新可见行异常则整源失败；不改用 QVIX |
| ICE DXY/USDX | ICE 管理、对欧元等六种货币固定权重几何平均的美元指数 | P1-L：ICE 指数/授权行情；官方定义见 [ICE USDX](https://www.ice.com/forex/usdx) | 实时计算；具体行情/历史许可依合同 | 未接入，等待许可或授权供应商 | 没有许可时字段为 `DXY_UNAVAILABLE`；不得以 FRED broad dollar 静默代替 |
| FRED DTWEXBGS | 美联储“Nominal Broad U.S. Dollar Index”，广义贸易加权、日频、Jan 2006=100 | P0-O：[FRED DTWEXBGS](https://fred.stlouisfed.org/series/DTWEXBGS)；API 访问规则见 [FRED API](https://fred.stlouisfed.org/docs/api/fred/series_observations.html) | 日频但存在发布滞后；以 FRED `updated_at`/release calendar 为准 | 待实现为低频宏观特征 | 必须显示全名和观测日期；**它不是 DXY**。只能建立独立特征 `FED_BROAD_DOLLAR`，不能填入 `DXY` 字段 |
| FTSE/CoreCommodity CRB | LSEG/FTSE 管理的 CoreCommodity CRB 商品篮子基准 | P1-L：LSEG 数据平台、直接 feed/API 或授权分销商；定义见 [LSEG CRB 指数页](https://www.lseg.com/en/ftse-russell/indices/commodity-indices) | 依许可 feed | 未接入 | 免费资讯站同名值仅作 P2-R 核验线索；若未确认代码、币种、总收益/价格版本和授权，不得进入评分 |
| Baltic Dry Index（BDI） | Baltic Exchange 管理的干散货运价综合基准 | P1-L：Baltic 订阅/API/授权供应商；许可要求见 [Baltic Market Data](https://www.balticexchange.com/en/data-services/Methodology/market-data.html) 和 [Data Policy](https://www.balticexchange.com/en/site-services/data-policy.html) | 工作日发布，准确时刻和使用权依订阅 | 未接入 | AKShare/资讯商可作 P0-S/P2-R 的显示级降级，但不能声称拥有官方自动化使用许可；航运股指数不能替代 BDI |
| 全球股指/期货/商品 | 各官方市场的同一交易日收盘或明确时点快照 | P1-L 官方/授权 feed；P0-S AKShare 公开资讯接口 | 按各市场时区 | 已有跨市场二手快照；覆盖与历史仍在补齐 | 每个品种独立失败；名称/代码严格白名单；缺失不能由搜索命中相似名称补位；相关性只描述统计共振，不写成因果 |

### 2.5 新闻、公告和宏观事件

| 数据项 | 精确语义 | 首选与降级来源 | 发布时间/时点 | 当前状态 | 失败与回退规则 |
| --- | --- | --- | --- | --- | --- |
| 公司公告 | 交易所披露的原文公告、修订公告及附件 | P0-O：上交所、深交所、北交所官方披露 | 以官方页面发布时间为准 | 已有新闻证据框架，官方公告覆盖待扩充 | 同一公告修订版覆盖旧版结论但保留版本链；摘要不能代替原文证据 |
| 政策/宏观 | PBOC、SAFE、NBS、财政部、发改委等官方发布 | P0-O 官方站点；P0-S 权威媒体转述只作抢先提示 | 以官方发布时间为准 | 进行中 | 媒体抢发先标 `UNCONFIRMED`；官方原文到达后再进入模型评分 |
| 市场新闻 | 有作者、发布时间、正文、来源 URL 的财经报道 | P0-S：合规可达的主流财经网站/RSS | 以页面发布时间并记录抓取时间 | 已实现基础采集 | 去重、正文哈希、跨源核验；标题党、转载链不作为多个独立证据 |
| 社交情绪 | 帖子、群聊、热榜等非结构化舆情 | P2-R | 实时 | 暂不进入决策 | 只生成待核线索；不得把数量/转发直接解释为基本面事实 |

## 3. 关键口径不可互换清单

以下规则是硬约束，不由模型自由判断：

1. `ICE DXY != FRED DTWEXBGS`。前者是 ICE 六币种固定权重指数，后者是美联储广义贸易加权美元指数；二者可以并列，不能互填。
2. `Cboe VIX != QVIX != 单合约隐含波动率`。VIX 使用 SPX 期权并代表约 30 日美国股市预期波动；QVIX 是国内提供方构造序列；上交所合约 IV 是单合约风险值。
3. `FR007 != FDR007 != DR007`。FR/FDR 是中位定盘；DR007 是另一个实际成交统计口径。
4. `ETF 供应商“主力净流入” != ETF 一级市场净申购 != ETF 份额变化`。前者是供应商订单分类；后两者需要官方申赎/份额数据并处理基金事件。
5. `北向成交总额 != 北向净买入`。2024-08-19 后公开日度方向性净额结构性缺失，不能从总额、前十成交或季度持仓推算。
6. `CRB 精确基准 != 自选商品篮子`，`BDI != 航运股票指数`。可以建立另名代理特征，但不得沿用受管基准名称。
7. `中间价 != 在岸即期收盘 != 离岸 USD/CNH`。三者必须分字段、分发布时间和分市场。
8. `IOPV != NAV`。IOPV 是盘中参考估值，基金净值是基金会计口径；报告必须分别标示。

## 4. 分阶段数据装配与报告更新时间

系统不应在 15:00 一次性等待所有数据，而应生成可追溯的多版本报告。每个版本记录 `report_as_of`、输入观测日、首次可见时间和缺失清单；后续版本追加/修订，不悄悄覆盖此前证据。

| 阶段 | 必须尝试的数据 | 允许使用的最近时点 | 产物与门控 |
| --- | --- | --- | --- |
| 15:10 收盘快照 | A 股完成日线/分钟线、ETF 二手快照、沪深 300 现货、当日已发布 Shibor/FR/FDR、IF 日数据（若已出现）、东财→腾讯收盘宽度、当日 09:15 SAFE 中间价、最近完成的 Cboe VIX | 中债曲线只能取 T-1；VIX 取最近已完成美股交易日；盘后网页尚未发布的字段保持缺失 | 生成“初版技术与流动性快照”；所有来源标 `official/secondary/stale/degraded`；不能因字段未到把值写 0 |
| 16:20 衍生品补丁 | 上交所期权风险指标/合约列表、ETF 盘后规模/PCF（若已发布）、沪深两融初步数据（若已发布） | 只接受查询日等于 T 的数据；页面未承诺 SLA 时以首次成功轮询为 `available_at` 下界 | 派生 ATM IV/skew/term 时保存算法和选约；未齐备就保持不可用，不输出伪 QVIX |
| 17:40 利率曲线补丁 | 17:30 发布的 T 日中债国债收益率曲线 | T 日精确曲线；若官方尚未更新仍保留 T-1 并标 stale | 重算 1Y/10Y/30Y、10Y−1Y、30Y−10Y 和日变动；只更新受影响的宏观结论 |
| 18:10 官方盘后对账 | 中金所 IF 官方日文件、上交所/深交所两融、ETF 官方规模与交易所统计、中证指数/成份公告 | T 日已发布数据；未发布来源继续轮询，不假定 18:10 必然齐全 | 对 ETF 供应商快照和官方盘后数据做差异告警；IF 基差仅在 CSI300 T 日收盘齐备时生成 |
| 20:10 收盘深度版 | 官方公告、政策原文、主流新闻去重集、行业/主题关联、所有已到盘后数据 | 截至 20:10 的 PIT 证据 | 调用大模型生成最终收盘深度研究；模型只能引用证据编号对应的人类可读来源标题/链接；缺失项进入“不确定性” |
| 次日盘前 | 完成的美股/欧洲市场、Cboe VIX、全球期货/商品、人民币与美元指标、隔夜公告新闻；09:15 后追加当日 SAFE 中间价 | 美国数据按 New York 交易日；08:30 左右的报告只能用当时已完成数据；当日中间价在 09:15 前不可用 | 生成盘前风险修订；如 09:15 后运行，补当日中间价；集合竞价/开盘前再次检查停牌、公告修订与隔夜跳空，不自动继承昨晚价格条件 |

推荐调度器把阶段时间当成“开始轮询时间”，而不是“数据必然可用时间”。对没有官方发布时间承诺的网页，应采用带抖动的有限重试，并将第一次成功看到 T 日数据的本地时间记录为 `first_seen_at`。

## 5. 统一的时间、缓存和质量门槛

每条决策证据至少保存以下字段：

- `source_id`：稳定的来源和接口标识，不只写“AKShare”。
- `source_url`：人可访问的官方或供应商页面。
- `provider` 与 `upstream_provider`：例如读取层是 AKShare、上游是 ChinaMoney；二者不能只留一个。
- `semantic_name`：完整指标名，如“FDR007 银银间回购定盘利率”。
- `observation_date` / `observed_at`：该值描述的交易日或观测时点。
- `available_at`：依据官方发布时间或首次成功观测确定的可知时点。
- `fetched_at`：本机实际抓取时间。
- `timezone`：来源时区；禁止无时区 datetime。
- `content_sha256`、`etag`、`last_modified`：能取得时用于缓存重验与修订追踪。
- `stale`、`degraded`、`failure_code`：显式数据质量状态。
- `unit`、`currency`、`adjustment`：百分比/小数、人民币/美元、复权方式等。

决策门槛：

1. 只有 `available_at <= report_as_of` 的证据可进入该版报告，防止未来数据泄漏。
2. 同一证券同一日出现多个来源时，不盲目平均；先比对定义。官方公开源和已授权的官方/许可 feed 优先于 P0-S，二者之间按时效、完整性和合同口径配置；付费与否不代表数据质量高低。许可源必须保留合同、供应商和代码映射。
3. 缺失显示为“未获得/尚未发布/结构性不可获得”，数值层保存 `null`；不得用 `0`。
4. 报告展示通常保留 3 位小数，但计算与存储保留原始精度。比例必须注明 `%` 或倍数，避免 `0.012` 与 `1.2%` 混淆。
5. 任一接口超时只降级该数据族，不能令整份收盘报告失败。
6. 供应商网页 schema 改变、字段重复、代码模糊命中、日期不匹配或单位不明时，按 payload/schema failure 处理，不能让大模型猜值。
7. 回测必须读取历史缓存与当时生效的成份/公告版本；当前网页不构成历史 PIT 数据库。

## 6. 降级决策树

对每个数据项按以下顺序执行：

1. 尝试 P0-O 官方源，并验证代码、日期、单位、时区和 schema。
2. 官方源短暂失败时，使用同一内容哈希/ETag 已验证缓存；若超过新鲜度阈值则标 stale。
3. 存在**同语义** P0-S 时才降级读取，并标明二手供应商、上游来源和降权。
4. 只有近似代理时，建立不同字段名并列入 P2-R，不填充原字段。
5. 没有可靠数据时返回明确 failure code 和 `null`，报告解释其对结论的影响。

典型禁止降级路径：

- DXY 缺失 → 不得填 FRED broad dollar；可新增 `FED_BROAD_DOLLAR`。
- VIX 缺失 → 不得填 QVIX；可分别报告国内派生 IV。
- CRB/BDI 无许可 → 不得从随机网页抓同名数值进入评分。
- 北向净买入缺失 → 不得从北向成交总额、ETF 成交或季度持仓推算。
- CSI300 现货缺失 → IF 基差为不可计算，而不是以 510300 ETF 价格替代。
- ETF 官方申赎缺失 → 不得以“主力净流入”替代。

## 7. 当前落地结论与下一批实现顺序

当前已经完成且可作为收盘报告证据的新增数据族：

1. **ETF context**：精确代码行、最新价、IOPV、折溢价、换手、份额、成交额、盘口和供应商资金字段；供应商资金字段已与申赎语义隔离。
2. **Liquidity context**：FR 与 FDR 两个独立失败域，以及中债国债收益率曲线；明确不把 FDR007 写成 DR007。
3. **IF context**：中金所 IF 各合约日行情；只有同日 CSI300 现货存在才计算基差。
4. **Official rates**：SAFE 美元兑人民币中间价与 Shibor 官方八期限历史，均按发布时间做 PIT 过滤。
5. **SSE official ETF/option**：上交所 ETF 结算后总份额和逐合约期权 Greeks/官方 IV；报告保留真实统计日和官方零值。
6. **Cboe VIX**：官方日线 CSV、New York 时区/PIT、缓存校验与历史坏行隔离已经接入。
7. **A 股市场广度**：东财主源、腾讯回退和沪深京完整性门槛已经接入。

仍在进行：VIX 持久缓存与阶段调度；市场广度历史 PIT 归档、涨跌停规则和站上均线比例；SSE 期权 ATM 选约/偏度/期限结构；ETF PCF/NAV 与跨日份额变化。

建议后续顺序：

1. 为 09:15/11:00/11:30 官方数据建立按时段调度、内容缓存和连续可用性监控。
2. 补齐期权合约到期日/行权价/流动性筛选，派生并回测 510300 ATM IV、skew、term structure；不要采用口径不明的 QVIX 替代。
3. 接入沪深两融、ETF PCF/NAV 和跨日官方份额变化，完成 18:10 官方对账。
4. 从现在开始按日归档 CSI 成份、行业映射和全市场广度，为未来 PIT 回测积累可信历史。
5. 若用户愿意购买数据许可，再评估 ICE DXY、LSEG CRB、Baltic BDI 和交易所实时 Level-1/Level-2；未取得许可前保持明确缺口。

## 8. 官方来源索引

- Cboe：[VIX 历史日线](https://www.cboe.com/tradable_products/vix/vix_historical_data)、[指数实时数据接入](https://www.cboe.com/us/indices/accessing-index-data/)
- SAFE/PBOC：[人民币汇率中间价数据](https://www.safe.gov.cn/safe/rmbhlzjj/)、[09:15 发布规则](https://www.safe.gov.cn/safe/2014/0702/5725.html)、[16:30 即期收盘发布时间](https://www.safe.gov.cn/safe/2022/1230/22197.html)
- Shibor：[官方定义与 11:00 发布](https://www.shibor.org/chinese/llshibor/)
- ChinaMoney：[FR/FDR 定义与 11:30 发布](https://www.chinamoney.com.cn/chinese/bkfrr/)、[产品指南](https://www.chinamoney.com.cn/dqs/cm-s-notice-query/fileDownLoad.do?contentId=3092650&mode=open&priority=0)
- ChinaBond：[国债及其他债券收益率曲线与 17:30 发布说明](https://yield.chinabond.com.cn/cbweb-pbc-web/pbc/more?locale=cn_zh)
- 中证指数：[官网](https://www.csindex.com.cn/)、[沪深 300 编制与发布方案](https://oss-ch.csindex.com.cn/static/html/csindex/public/uploads/indices/detail/files/zh_CN/000300_Index_Methodology_cn.pdf)
- CFFEX：[日行情数据](https://www.cffex.com.cn/fzjy/mrhq/)、[沪深 300 股指期货合约](https://www.cffex.com.cn/cn/hs300.html)
- SSE：[对外公示数据目录](https://www.sse.com.cn/market/publicdata/)、[ETF 市场数据](https://etf.sse.com.cn/marketdata/)、[期权风险指标](https://www.sse.com.cn/assortment/options/risk/)、[融资融券明细与汇总](https://www.sse.com.cn/market/othersdata/margin/detail/index.shtml)
- SZSE/HKEX：[深交所 2024-08-19 披露调整通知](https://www.szse.cn/szhk/hkbussiness/news/t20240726_608353.html)、[HKEX 联合公告](https://www.hkex.com.hk/News/Market-Communications/2024/2404122news?sc_lang=en)、[HKEX 实施通告](https://www.hkex.com.hk/-/media/HKEX-Market/Services/Circulars-and-Notices/Participant-and-Members-Circulars/HKSCC/2024/ce_HKSCC_NOM_217_2024.pdf)
- ICE/FRED：[ICE USDX 定义](https://www.ice.com/forex/usdx)、[FRED Broad Dollar DTWEXBGS](https://fred.stlouisfed.org/series/DTWEXBGS)
- LSEG/Baltic：[FTSE/CoreCommodity CRB](https://www.lseg.com/en/ftse-russell/indices/commodity-indices)、[Baltic Market Data](https://www.balticexchange.com/en/data-services/Methodology/market-data.html)
