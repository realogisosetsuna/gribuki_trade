# 策略实验室设计与使用边界

## 定位

`gribuki_trade.strategy_lab` 是离线研究框架，不是在线自动调参器，也不会修改
`RecommendationGateConfig`、候选池、PAPER 账户或实盘账户。研究结果进入生产策略前，必须经过
独立复核、版本冻结和显式发布。

开发计划中“基于回测迭代评分权重”和“技术面探索引擎”的方向合理，但若直接依据全历史最优
结果在线改权重，会产生未来函数、数据窥探和多重试验偏差。当前实现先建立以下安全基线：

- 冻结数据 manifest：内容 SHA-256、字段、标签、时间范围、来源版本和冻结时间；
- 冻结策略 manifest：策略版本、代码 revision、参数及因子表达式；
- expanding walk-forward；训练与验证之间有 purge，每次验证后有 embargo；
- 单独保留最终测试集。候选选择只能读取 validation 指标，test 仅在选择锁定后评估；
- 同时记录训练、验证、最终测试、预登记基线及多种成本情景；
- 验证目标采用各成本情景中的最差表现，不用低成本情景掩盖脆弱性；
- 权重位于 simplex 上、和为 1；宏观权重上限 40%，单个技术家族也有上限；
- 保存试验总数、每折指标、解释性家族贡献，以及多重假设和搜索空间过大的警告；
- SQLite 表为 append-only，同一实验 ID 的不同内容会触发冲突错误。

## 安全因子 DSL

因子表达式不是 Python 代码，不调用 `eval`。解析器仅接受：

- 列：`open`、`high`、`low`、`close`、`volume`、`amount`；
- 算术：`+`、`-`、`*`、`/`；
- 函数：`lag(series,n)`、`return(series,n)`（或 `ret`）、`ma`、`vol`、`zscore`；
- 有界整数窗口和数值常量。

属性访问、下标、推导式、lambda、导入、任意函数、幂运算和动态窗口都会被拒绝。除零、非有限
输入和非有限结果失败关闭。每个表达式都导出 `required_warmup`；历史不足时不能计算，缺失值也
不会被填成中性值。

示例：

```python
from gribuki_trade.strategy_lab import (
    compile_factor_expression,
    evaluate_factor_expression,
)

factor = compile_factor_expression(
    "zscore(return(close, 1), 20) + ma(volume, 5) / ma(volume, 20)"
)
evaluation = evaluate_factor_expression(
    factor,
    {"close": closes, "volume": volumes},
)
```

## 尚未自动化的环节

当前模块提供可审计的实验协议、权重搜索、因子语言和存储，并已提供单证券、long/cash 的
A 股日线 `StrategyEvaluator`。它不会凭空生成可靠标签，也不是完整的横截面组合回测器；调用方
仍需提供冻结的 PIT 样本、显式价格限制和正确费用场景。公司行动、退市证券、跨证券组合、
真实成分历史与更精细的成交模型仍需后续数据和实现。此外，当前不会：

- 用 PAPER 或实盘结果持续在线更新权重；
- 把 LLM 生成的表达式直接发布为策略；
- 根据最终测试集继续选择候选；
- 将一次显著结果解释为真实 alpha。

后续应增加数据快照构建器和统一事件驱动回测器，然后做组合层容量/换手约束、横截面中性化、
deflated Sharpe / PBO 等统计诊断。每轮新搜索都需要新的未见数据或 shadow 期，不能反复消费同一
最终测试集。

## 受控候选发现

`strategy_lab.discovery` 在安全 DSL 之前再加一层有界、版本化的模板 grammar。默认
`technical-factor-grammar@1` 只组合预先登记的经济含义家族：动量、趋势、均值回归、波动率、
流动性和量价确认。窗口只能来自显式白名单；模板本身、模板版本、窗口域、列域和复杂度上限都
进入 grammar SHA-256。

候选生成具有以下约束：

- 候选按惰性笛卡尔积生成；发现第 `max_trials + 1` 个组合时立即抛出预算异常，不返回部分结果，
  也不会为了统计一个巨大空间而耗尽内存；引擎另设 100,000 次绝对上限；
- 每个参数组合都计入 `trial_count`，DSL 拒绝和规范化重复也保留独立审计记录；
- 通过 AST 规范化表达式后去重，候选和尝试都有稳定 ID；
- 每个候选带经济家族、模板版本、参数、warmup、AST 节点数和表达式摘要；
- 多于一个独立候选时显式产生多重假设检验警告；
- 可选相关性过滤是纯函数，只接受调用方提供的开发集数值；缺失、样本不足、零方差、长度错误
  和高度相关均有稳定拒绝原因。

候选发现不执行收益回测、不访问最终 holdout、不调用 LLM，也不会写入线上配置或自动发布。
相关性过滤的输入必须由调用方限定为训练/验证数据；最终测试数据不可用于决定保留哪个因子。

## A 股日线确定性评价器

`strategy_lab.ashare_evaluator.AShareDailyStrategyEvaluator` 已将冻结的日线信号样本接入
`StrategyEvaluator` 协议，使 walk-forward 权重实验可以评价 long/cash 资金曲线，而不再只能依赖测试替身。边界如下：

- 每个评价器只接受一个证券的完整时间序列；跨证券组合必须先提供完整的横截面价格面板，不能拿候选行冒充持仓估值数据；
- 每条技术家族分、宏观分都携带 `known_at` 和来源 revision，且必须在 `signal_as_of` 前已知；下一完整交易日 bar 只用于撮合已经决定的订单及收盘估值；
- 数据内容 SHA-256、每条 observation 的技术/宏观/bar 来源 revision，以及策略代码、参数和因子表达式都进入 manifest；
- 默认按下一交易日开盘模拟，也可启用保守开盘限价规则；停牌、价格带缺失、封板、量能不足均不成交，绝不根据代码猜测 ST、板块或涨跌停比例；
- 买入必须为 100 股整数手，卖出允许处理剩余股；执行 T+1、现金门禁、成交量参与率、佣金最低额、滑点、过户费及卖出税；
- 股票与 ETF 使用调用方明确登记的 `CostScenario`。ETF 场景必须显式设置 `tax_bps=0`，评价器不会暗中覆盖错误税率；
- `PerformanceMetrics.family_contributions` 在该适配器中表示“平均加权决策分贡献”，用于解释权重如何形成信号，并不是收益归因；
- 每次调用独立从现金开始，适合 train/validation/test 隔离评价；它不调参、不读取 holdout 做选择、不发布策略，也不连接 PAPER 或真实券商。

典型冻结流程先用 `canonical_ashare_evaluation_content(observations)` 生成内容摘要，再用
`ashare_evaluation_source_revisions(observations)` 生成逐 observation 血缘，最后创建
`DataManifest.freeze(...)`。这样同一供应商按交易日变化的 revision 不会产生 manifest 键冲突。
