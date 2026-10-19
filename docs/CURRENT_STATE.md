# Current Gateway state

更新日期：2026-10-19。执行 BinanceMarketData v0.4 总计划及 Gateway 模块计划，保留v0.3已完成事实。G3-a二十产品离线扩容、48 contexts、启动/恢复与路由已完成验证、独立复核及production CI，PR #38交付。当前导航见 [MILESTONES](MILESTONES.md)，证据见 [G0 v0.3](GATEWAY_G0_V03_EVIDENCE.md)、[G1 v0.3](GATEWAY_G1_V03_EVIDENCE.md)、[G2 v0.3](GATEWAY_G2_V03_EVIDENCE.md) 和 [G3-a离线](GATEWAY_G3_OFFLINE_EVIDENCE.md)。旧阶段编号及 [2026-10-04 审查](CODE_REVIEW_2026-10-04.md)仅作实现/修复历史，不与新版 G0 混同。

```text
G0_THROUGH_G11=COMPLETE
G12-A=COMPLETE
G12-B=COMPLETE
G12-C=COMPLETE
M1_REVIEW_AND_BASELINE=COMPLETE
M2_CONFIGURED_PRODUCTION=IMPLEMENTED_OFFLINE_VERIFIED
V03_G0=COMPLETE_REVIEWED_MERGED
V03_G1=COMPLETE_OFFLINE_VERIFIED_REVIEWED
V03_G2=COMPLETE_OFFLINE_VERIFIED_REVIEWED_MERGED
V03_G3=G3A_G3B_OFFLINE_COMPLETE_G3C_OPEN
V04_G3A=COMPLETE_OFFLINE_VERIFIED_REVIEWED_CI_PASSED
BMD_RL_01=OFFLINE_COMPLETE_LIVE_OBSERVATION_PENDING
V04_G3B=COMPLETE_OFFLINE_VERIFIED_REVIEWED_CI_PASSED_MERGED
M3_FOUR_PRODUCT_ACCEPTANCE_AND_PRODUCTION_CI=COMPLETE_OFFLINE_V03_G2_SCOPE
M4_PERFORMANCE_STABILITY_LIVE_ACCEPTANCE_AND_DELIVERY=NOT_STARTED
PERFORMANCE_AND_RESOURCE_BUDGETS=NOT_ESTABLISHED
MULTI_PRODUCT_PERFORMANCE_ACCEPTANCE=NOT_COMPLETE
PROJECTION_MULTI_PRODUCT_API_CHANGE_REQUIRED=NO
PROJECTION_P1_TWENTY=OFFLINE_MEASUREMENT_REVIEWED_COMPLETE
PROJECTION_PERFORMANCE_ASSESSMENT=P1_OFFLINE_MEASUREMENT_COMPLETE_BUDGET_DECISION_PENDING_G3
CURRENT_PLAN=docs/MILESTONES.md
PRODUCTION_DAEMON=bmd-gatewayd
PRODUCTION_CONFIG=--config PATH
CONFIGURED_PRODUCT_COUNT=1..20
GLOBAL_TRACKED_STREAMING_CONTEXT_LIMIT=48
INSECURE_GRPC_LISTEN=NUMERIC_LOOPBACK_ONLY
OWNER_OBSERVATION_BUDGET=1_SECOND
STATUS_OBSERVATION_BUDGET=MIN_1_SECOND_CLIENT_DEADLINE_SHARED_ACROSS_PRODUCTS
TRANSPORT_MODEL=INDEPENDENT_PER_MARKET_KEY
INITIAL_READINESS=ALL_CONFIGURED_PRODUCTS_LIVE_SYNCHRONIZED
LATER_FAILURE=PRODUCT_LOCAL
HOT_RELOAD=NO
FOUR_PRODUCT_LIVE_ACCEPTANCE=NOT_COMPLETE
TWENTY_PRODUCT_PRODUCTION_CAPACITY=NOT_ESTABLISHED
```

生产入口已有 JSON 配置、按市场获取 metadata、逐产品数值规格解析、可配置产品组合、共享首次就绪截止时间、启动回滚和独立恢复。没有固定 BTCUSDT allowlist；普通路由当前支持精确的大写 ASCII 字母/数字 symbol，仍必须通过币安 metadata 验证，不能据此宣称支持币安任意 Unicode symbol。

已有 gRPC：两个市场的 `SubscribeOrderBook`；Spot 的三类事件与 USD-M 的深度事件；动态产品列表的 `GetGatewayStatus`。`SubscribeMarketState` 未实现。每产品 order-book 与 Event admission 各最多 8，普通队列容量各 64；进程 tracked streaming context 总上限 48。

G3-a离线画像为20 Key各一条Book和一条Depth Event，共40 contexts；已验证48满额、
第49条RESOURCE_EXHAUSTED、取消后复用和Status继续响应。保留48上限；全部已支持流
同时订阅需60，超出现行容量。20私有owner/精度/路由、全体首次Live、20个失败位置
整体回滚、迟到Live仍遵守绝对deadline、局部恢复及两轮全20产品恢复均通过。
研究样本配置见 [gateway-twenty.json](../examples/gateway-twenty.json)，不是最终部署清单。
G3-b联合API预算与G3-c共享VPS/实网/资源预算仍OPEN。

G1 修正 USD-M Depth `T` 为 transaction-time，trade-time 留空，零/缺失 presence 保持。
owner 独立记录最后 Applied Depth（UTC/monotonic/provenance），重复/缓冲/Gap 不刷新，reset 清除。
从本版起 Status `markets[].last_event_utc_ns` 明确收窄为当前有效 generation 最后 Applied
Depth receive UTC；缺少来源/时间或簿失效时 absent。历史任意流活跃时间仍内部保留，
不再由此 RPC 表达。客户端须检查墙钟回退；本轮提供时间证据，不承诺 age/SLA。
没有新 Wire/服务/线程或安静市场自动重同步。共同 fixture、真实 parser→Projection→
loopback Book/Event/Status、sanitizer 与独立审查已通过，真实四产品行情/VPS 验收留 G3。

四产品配置示例见 [examples/gateway.json](../examples/gateway.json)。G2 四产品离线组合、
故障/慢消费者/48 contexts/回滚和活动 RPC 信号退出已通过本机生产全图与 sanitizer；
最小 production CI 已通过 Linux/GCC13 生产图 21/21，独立复核及 PR #37 合并完成；
main 固定二进制缓存和普通 main/PR 消费路径已验证，见 [G2 证据](GATEWAY_G2_V03_EVIDENCE.md)。
真实行情及 VPS/API/性能验收仍属 G3。历史固定双 BTC 的真实验收和性能基线不能直接作为此版本多产品验收或容量证明。

2026-10-04 已修复 owner 线程创建异常清理死锁、旧验收客户端参数/serving 字段不匹配、官方 combined `serverShutdown` 识别问题。测试 hook 只位于已有内部 RuntimeTestOptions，不进入配置或生产流程。

v0.3 G0 已将无认证 gRPC 限为数值 IPv4 127/8 或 IPv6 ::1，拒绝 wildcard、非 loopback 和 hostname；配置入口、直接生产构造与实际绑定均检查。交易所 JSON 容忍无关扩展，仍校验必需字段/类型、精确身份和数值类型；Spot 不接受 USD-M 的 pu，序列有效性仍由 Projection 判断。自身配置继续拒绝未知字段。

owner FIFO 观察使用 deadline/stop token，超时不返回陈旧 Live。Status 整次请求共享最多一秒等待，服务关闭取消观察；启动 readiness 非阻塞检查，不因 owner 暂停越过原来的全产品绝对截止时间。G10 测试用 RAII join 与有界同步。真实正在执行的 owner 操作仍需返回后 join，未 detach 或提前销毁产品；这不是强制中断任意阻塞回调的能力。

默认 CMake 仍为独立 Foundation 图；生产图必须显式启用。GitHub 已补单一 production
Release/offline job，普通 PR 只用锁定二进制缓存；手动准备入口才允许构建固定上游。
G2 本机完整生产图及必要 sanitizer、GitHub/main/PR 二进制消费实跑均通过。

高性能、安全稳定、低延迟与低 CPU/内存消耗是必需交付目标，尚未完成多产品质量验收。Projection 已在生产路径使用；多交易对不需要新增 Projection API，但其每批全簿复制/重建应进入性能评估。是否修改 Core 由测量与目标预算决定，见 [配套评估](PROJECTION_INTEGRATION_REVIEW_2026-10-04.md)。

Projection P1（2026-10-18）复用 G2 fixture 测四产品成本，增加可选 Adapter/checked apply/
owner CPU/Write 点位及精确多产品导出；普通构建语义不变。离线结果与边界由 Projection
`docs/PROJECTION_P1_RESULT.md` 共同记录。该工作不授予 G3/VPS/API 联合预算资格。

2026-10-19 用户追加授权的20keys离线P1已完成：18次异构成对实验交付127,920更新/255,840客户端消息，独立结果/原始证据审查APPROVED。未扩大48 contexts或产品级8 slots；P2仍CONDITIONAL，G3共享VPS/API预算未验收。技术提交`7266c47`的Linux production 24/24与全部七项普通CI通过（run `39145975891`）；该提交只对实测`8ce41be`补强测试与可移植命名空间查找，production/measurement代码不变。报告/原始包由Projection PR #55记录。


## 2026-10-19 G3-b shared-IP offline implementation

- [x] Gateway fixed rolling shares3000 Spot/600 USD-M weight per minute and60
  WS attempts per5min per market; actual request/connection admission and shared
  429/418 gate, bounded metadata and clean cancellation implemented.
- [x] Recorder companion policy and five two-process composition scenarios PASS;
  production binding/stop tests and existing20-product recovery cover separate layers.
- [x] New scoped code review APPROVED; complete Release27/27, format/diff PASS.
- [x] Full ASan27/27, focusedTSan4/4 andUBSan4/4 PASS. All seven ordinary
  push/PR CI jobs pass (39250188012/39250228864); Linux production25/25.
  PR#39 merged as `8d171c1`, companion Recorder#82 as `d98821c`; both merge
  trees match the tested heads. G3-b/R3-b offline COMPLETE / MERGED.

[Evidence](GATEWAY_G3B_SHARED_API_EVIDENCE.md) states assumptions and validation.
G3-c/ordinary live observation, actual metadata/current IP users, target VPS
resources and BMD-4 remain OPEN. No production/VPS/R2 operation occurred.
