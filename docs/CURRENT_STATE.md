# Current Gateway state

更新日期：2026-10-04。当前计划见 [MILESTONES](MILESTONES.md)，本次审查与测试范围见 [CODE_REVIEW](CODE_REVIEW_2026-10-04.md)。旧阶段文档仅作为历史证据。

```text
G0_THROUGH_G11=COMPLETE
G12-A=COMPLETE
G12-B=COMPLETE
G12-C=COMPLETE
M1_REVIEW_AND_BASELINE=COMPLETE
M2_CONFIGURED_PRODUCTION=IMPLEMENTED_OFFLINE_VERIFIED
M3_FOUR_PRODUCT_ACCEPTANCE_AND_PRODUCTION_CI=NEXT
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
TRANSPORT_MODEL=INDEPENDENT_PER_MARKET_KEY
INITIAL_READINESS=ALL_CONFIGURED_PRODUCTS_LIVE_SYNCHRONIZED
LATER_FAILURE=PRODUCT_LOCAL
HOT_RELOAD=NO
FOUR_PRODUCT_LIVE_ACCEPTANCE=NOT_COMPLETE
EIGHT_PRODUCT_CAPACITY=NOT_ESTABLISHED
```

生产入口已有 JSON 配置、按市场获取 metadata、逐产品数值规格解析、可配置产品组合、共享首次就绪截止时间、启动回滚和独立恢复。没有固定 BTCUSDT allowlist；普通路由当前支持精确的大写 ASCII 字母/数字 symbol，仍必须通过币安 metadata 验证，不能据此宣称支持币安任意 Unicode symbol。

已有 gRPC：两个市场的 `SubscribeOrderBook`；Spot 的三类事件与 USD-M 的深度事件；动态产品列表的 `GetGatewayStatus`。`SubscribeMarketState` 未实现。每产品 order-book 与 Event admission 各最多 8，普通队列容量各 64；进程 tracked streaming context 总上限 48。

四产品配置示例见 [examples/gateway.json](../examples/gateway.json)。配置范围的离线检查已实现，完整四产品生产组合、CI runtime 覆盖及真实行情验收仍按 M3/M4 执行。历史固定双 BTC 的真实验收和性能基线不能直接作为此版本多产品验收或容量证明。

已修复本次复现的 owner 线程创建异常清理死锁、旧验收客户端参数/serving 字段不匹配、官方 combined `serverShutdown` 识别问题。测试 hook 只位于已有内部 RuntimeTestOptions，不进入配置或生产流程。

默认 CMake/现有 GitHub CI 仍为 Foundation 图；生产图必须显式启用。此次本机验证覆盖生产图，CI 补齐是 M3 的必要交付项。

高性能、安全稳定、低延迟与低 CPU/内存消耗是必需交付目标，尚未完成多产品质量验收。Projection 已在生产路径使用；多交易对不需要新增 Projection API，但其每批全簿复制/重建应进入性能评估。是否修改 Core 由测量与目标预算决定，见 [配套评估](PROJECTION_INTEGRATION_REVIEW_2026-10-04.md)。
