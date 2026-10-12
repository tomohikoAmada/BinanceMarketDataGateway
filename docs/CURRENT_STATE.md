# Current Gateway state

更新日期：2026-10-12。执行 BinanceMarketData v0.3 总计划及 Gateway 模块计划；本轮仅关闭新版 Gateway G0，等待独立复核。当前导航见 [MILESTONES](MILESTONES.md)，本轮证据见 [G0 v0.3](GATEWAY_G0_V03_EVIDENCE.md)。旧阶段编号及 [2026-10-04 审查](CODE_REVIEW_2026-10-04.md)仅作实现/修复历史，不与新版 G0 混同。

```text
G0_THROUGH_G11=COMPLETE
G12-A=COMPLETE
G12-B=COMPLETE
G12-C=COMPLETE
M1_REVIEW_AND_BASELINE=COMPLETE
M2_CONFIGURED_PRODUCTION=IMPLEMENTED_OFFLINE_VERIFIED
V03_G0=IMPLEMENTED_OFFLINE_VERIFIED_PENDING_REVIEW
V03_G1=NOT_STARTED
V03_G2=NOT_STARTED
V03_G3=NOT_STARTED
M3_FOUR_PRODUCT_ACCEPTANCE_AND_PRODUCTION_CI=NOT_COMPLETE_V03_G2_SCOPE
M4_PERFORMANCE_STABILITY_LIVE_ACCEPTANCE_AND_DELIVERY=NOT_STARTED
PERFORMANCE_AND_RESOURCE_BUDGETS=NOT_ESTABLISHED
MULTI_PRODUCT_PERFORMANCE_ACCEPTANCE=NOT_COMPLETE
PROJECTION_MULTI_PRODUCT_API_CHANGE_REQUIRED=NO
PROJECTION_PERFORMANCE_ASSESSMENT=PLANNED
CURRENT_PLAN=docs/MILESTONES.md
PRODUCTION_DAEMON=bmd-gatewayd
PRODUCTION_CONFIG=--config PATH
CONFIGURED_PRODUCT_COUNT=1..8
GLOBAL_TRACKED_STREAMING_CONTEXT_LIMIT=48
INSECURE_GRPC_LISTEN=NUMERIC_LOOPBACK_ONLY
OWNER_OBSERVATION_BUDGET=1_SECOND
STATUS_OBSERVATION_BUDGET=MIN_1_SECOND_CLIENT_DEADLINE_SHARED_ACROSS_PRODUCTS
TRANSPORT_MODEL=INDEPENDENT_PER_MARKET_KEY
INITIAL_READINESS=ALL_CONFIGURED_PRODUCTS_LIVE_SYNCHRONIZED
LATER_FAILURE=PRODUCT_LOCAL
HOT_RELOAD=NO
FOUR_PRODUCT_LIVE_ACCEPTANCE=NOT_COMPLETE
EIGHT_PRODUCT_CAPACITY=NOT_ESTABLISHED
```

生产入口已有 JSON 配置、按市场获取 metadata、逐产品数值规格解析、可配置产品组合、共享首次就绪截止时间、启动回滚和独立恢复。没有固定 BTCUSDT allowlist；普通路由当前支持精确的大写 ASCII 字母/数字 symbol，仍必须通过币安 metadata 验证，不能据此宣称支持币安任意 Unicode symbol。

已有 gRPC：两个市场的 `SubscribeOrderBook`；Spot 的三类事件与 USD-M 的深度事件；动态产品列表的 `GetGatewayStatus`。`SubscribeMarketState` 未实现。每产品 order-book 与 Event admission 各最多 8，普通队列容量各 64；进程 tracked streaming context 总上限 48。

四产品配置示例见 [examples/gateway.json](../examples/gateway.json)。配置范围的离线检查已实现，完整四产品生产组合、CI runtime 覆盖及真实行情验收属新版 G2/G3（原 M3/M4），尚未完成。历史固定双 BTC 的真实验收和性能基线不能直接作为此版本多产品验收或容量证明。

2026-10-04 已修复 owner 线程创建异常清理死锁、旧验收客户端参数/serving 字段不匹配、官方 combined `serverShutdown` 识别问题。测试 hook 只位于已有内部 RuntimeTestOptions，不进入配置或生产流程。

v0.3 G0 已将无认证 gRPC 限为数值 IPv4 127/8 或 IPv6 ::1，拒绝 wildcard、非 loopback 和 hostname；配置入口、直接生产构造与实际绑定均检查。交易所 JSON 容忍无关扩展，仍校验必需字段/类型、精确身份和数值类型；Spot 不接受 USD-M 的 pu，序列有效性仍由 Projection 判断。自身配置继续拒绝未知字段。

owner FIFO 观察使用 deadline/stop token，超时不返回陈旧 Live。Status 整次请求共享最多一秒等待，服务关闭取消观察；启动 readiness 非阻塞检查，不因 owner 暂停越过原来的全产品绝对截止时间。G10 测试用 RAII join 与有界同步。真实正在执行的 owner 操作仍需返回后 join，未 detach 或提前销毁产品；这不是强制中断任意阻塞回调的能力。

默认 CMake/现有 GitHub CI 仍为 Foundation 图；生产图必须显式启用。此次本机验证覆盖生产图，CI 补齐是新版 G2 的交付项。

高性能、安全稳定、低延迟与低 CPU/内存消耗是必需交付目标，尚未完成多产品质量验收。Projection 已在生产路径使用；多交易对不需要新增 Projection API，但其每批全簿复制/重建应进入性能评估。是否修改 Core 由测量与目标预算决定，见 [配套评估](PROJECTION_INTEGRATION_REVIEW_2026-10-04.md)。
