# Gateway architecture

当前开发计划：[docs/MILESTONES.md](docs/MILESTONES.md)，依据 2026-10-12 BinanceMarketData v0.3 总计划及 Gateway 模块计划。2026-10-15 用户授权 G1 时间语义/Depth 新鲜度及独立复核；G0 reset 观察补修已复核。当前实现是支持启动配置的有限产品集合，已经复用 G12-B 的 owner set 和 registry；历史固定双 BTC 是其中一个配置场景。

## 职责与依赖

```text
Binance public REST / WebSocket
                |
                v
       BinanceMarketDataGateway
          +--> Contracts Protobuf / opt-in gRPC artifacts
          +--> Projection ProtoAdapter --> Core
```

Contracts 管理消息、服务与 wire 兼容性；Projection 管理定点数、订单簿、Spot/USD-M 序列判断、bootstrap/reset 和快照；Gateway 管理网络、metadata、时间戳、连接恢复/轮换、owner 调度、有限发布队列、订阅与 gRPC。Gateway 不依赖 Recorder，不复制 proto，不添加订单簿或序列分类器。

## 生产组合

```text
bmd-gatewayd --config PATH
  -> validate JSON (grpc_listen, spot_symbols, usdm_symbols)
  -> fetch exchangeInfo once per configured market
  -> resolve NumericSpec for every exact MarketKey
  -> ProductionGateway
       +-- ConfiguredProductRuntimeSet (1..8 stable owned ProductRuntime)
       |     +-- immutable MarketKey
       |     +-- MarketRuntime / private BookProjection / one owner thread
       |     +-- product-local order-book and event publication
       |     +-- independent RecoveryCoordinator / one active transport
       +-- immutable non-owning MarketRuntimeRegistry
       +-- one shared synchronous gRPC server
```

`MarketKey = (venue, market, exact symbol)`。同一 symbol 的 Spot 与 USD-M 是两个产品。配置不自动修改大小写；小写 stream route 只是网络编码。当前 Binance route 支持大写 ASCII 字母/数字，exchangeInfo 负责存在性、市场资格与精度验证。

配置严格拒绝未知字段、重复 JSON 字段、同市场重复 symbol、空总集合、超过八个产品和无效 endpoint。任何一个 symbol 的缺失、非交易状态、市场资格或不支持的数值规格都会使启动失败。配置验证不做网络 I/O；metadata 获取独立进行，并受网络阶段超时约束。变更配置需要重启，没有运行时 registry 增删。

无认证 gRPC 只接受数值 IPv4 127/8 或 IPv6 ::1；hostname、wildcard 和非 loopback 均拒绝。配置与直接构造检查非零端口；绑定边界重复校验，内部离线测试可用 loopback port 0。复用已有 Boost 地址解析，不做 DNS，不给 Foundation 加网络依赖。

交易所 JSON 只检查所需字段及已知语义，允许无关扩展对象/数组。Spot depth 的 pu 仍拒绝；USD-M pu 的类型/presence 保持已有 Adapter/Core 边界，Gateway 不增设序列分类器。自身配置的未知字段拒绝与上游协议扩展兼容是不同边界。

八个产品是资源政策，可在新容量证据后调整，不是币安限制。当前使用小集合线性查询和 stable unique_ptr owner；没有通用注册框架或共享多 symbol WebSocket。

## 并发与生命周期

- MarketRuntime 接收完整事件；有界 ingress 与 bootstrap buffer 分离；Projection 的读写、reset、publication 注册都在同一个 owner 上串行执行。对外只交付 owning/copy 数据。
- owner 线程成功创建后才发布 started 状态。生产 start 遇到返回失败或异常都回滚所有已启动产品。
- 所有配置产品共享一个首次就绪绝对截止时间；全体 Live/Synchronized 后绑定 gRPC。一个产品未就绪不能放行整体服务。
- 启动 readiness 对 owner FIFO barrier 做非阻塞观察，未完成就继续已有有界轮询。普通观察最多等待一秒；显式 deadline/stop token 观察返回 optional owning copy，无 mailbox 请求或返回后残留的调用者指针。超时不会伪报 Live。
- metadata 网络阶段与产品首次同步阶段分别有界，首次同步截止时间不因新增产品或观察到迟到的 Live 而重置。
- 运行后的产品故障隔离。每产品恢复先 quiesce 旧 transport，再在 owner 上 reset/rebootstrap；同时最多一个 active transport。
- 计划轮换沿用 23h50m monotonic 策略和 break-before-make；不拼接两条连接的数据。
- 订单簿 session 在需要完整 Projection rebootstrap 前结束；Event session 在实际 source generation 替换时结束，均不跨 generation 拼接。
- SIGINT/SIGTERM 使用 async-safe signal 模型。停止先关闭 admission、结束并取消/drain server handlers，再停止 transport/owner 和销毁产品；不得在 owner 已不存在时等待其确认。

## 发布与服务边界

`SubscribeOrderBook` 只发布 Projection 已 Applied 的数据。`SubscribeEvents` 需要恰好一个 V1 selector；Spot 支持 DIFF_DEPTH / AGG_TRADE / BOOK_TICKER，USD-M 仅 DIFF_DEPTH。深度事件为 PRE_PROJECTION_NORMALIZED，不代表已应用到订单簿。

每产品 order-book resident 与 Event active 各上限 8；各普通队列 64，另有一个 terminal slot。慢消费者按 session 终止。进程级 TryCancel tracker 总上限 48，与产品数无关；注册/取消握手保护 ServerContext 生命周期。昂贵 unary status 并发上限为一，不占 streaming tracker。status 采用已有 runtime/recovery/publication 观察，没有第二健康框架。

`GetGatewayStatus` 使用 min(一秒, client deadline) 的整次请求观察预算，多个产品共享同一 deadline；超时返回 DEADLINE_EXCEEDED，关闭服务取消观察。无限 RPC deadline 先在 system_clock 单位内截短再转换，避免 macOS 的时间单位转换溢出。服务 drain 后才销毁产品；正在执行的 owner 操作返回后仍须 join，不强行 detach。

`SubscribeMarketState` 尚未实现。服务直接使用 insecure gRPC，仅绑定数值 loopback。公开网络认证/TLS 如果成为实际部署需求，再增加专门交付项。

v0.3 G1 复用 Status 的 `markets[].last_event_utc_ns` 表示当前有效 generation 最后
Projection-Applied Depth 的 receive UTC；只有 runtime Live/Synchronized、recovery Live、
单 active transport 且来源匹配才有值，其余 absent/unknown，零仍有 presence。
这是从历史 G10“任意 normalized WebSocket 事件且跨恢复保留”含义的明确收窄；
transport 全流时间仍在内部 recovery observation 保留，消费者须更新解释。没有 Wire 变更。
owner 只在 Applied 提交可选 UTC/monotonic/provenance；重复/缓冲/Gap 不刷新，reset 清除。
时间证据不等于 freshness SLA；客户端墙钟年龄须检查回退，安静市场不自动 Gap/重同步。
USD-M 深度 E→event-time、T→transaction-time，trade-time absent，T=0/缺失各自保持。

## 构建与历史

Foundation 是独立、无网络无线程的历史最小配置/生命周期 seam；生产目标由 `BMD_GATEWAY_BUILD_PRODUCTION_DAEMON=ON` 启用。Contracts message-only 依赖与单独 gRPC artifact 的选择继续显式保留；G1 冻结 upstream smoke 不随 main 漂移而重新固定。

依赖不使用 floating FetchContent。Gateway 的 offline runtime 测试和 sanitizer 图已经可以本机构建；GitHub 现有默认 CI 只覆盖 Foundation，补生产 CI 是 M3 的要求。

旧 fixed-two 的 recovery 和 performance 记录保留为历史事实。当前配置生产代码的完成，不等同于四产品真实验收或容量证明。性能改动必须由实际瓶颈和同条件前后测量支持，不引入 speculative lock-free、busy polling、affinity、自定义 allocator 或 worker framework。

## 性能与质量目标

高性能、安全稳定、低延迟、低 CPU/内存消耗是交付要求。M3 准备可重复负载与正确性/稳定性回归，M4 定义硬件、负载和数值预算，完成完整链路性能验收。复用已有 instrumentation / Projection benchmark，不新增通用测量框架；阶段延迟、吞吐、分配、CPU/RSS 与队列指标共同评估。

Projection 当前为强异常保证在每个 accepted depth batch 中复制全簿并重建 candidate。这是性能候选路径；若实测影响预算，配套做局部事务更新优化并保留状态/序列/异常安全。多产品通过现有独立 Projection 实例组合，无需新增多 symbol API。详细证据见 [配套评估](docs/PROJECTION_INTEGRATION_REVIEW_2026-10-04.md)。
