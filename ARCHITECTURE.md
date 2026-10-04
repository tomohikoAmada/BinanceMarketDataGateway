# Gateway architecture

当前开发计划：[docs/MILESTONES.md](docs/MILESTONES.md)。当前实现是支持启动配置的有限产品集合，已经复用 G12-B 的 owner set 和 registry；历史固定双 BTC 是其中一个配置场景。

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

八个产品是资源政策，可在新容量证据后调整，不是币安限制。当前使用小集合线性查询和 stable unique_ptr owner；没有通用注册框架或共享多 symbol WebSocket。

## 并发与生命周期

- MarketRuntime 接收完整事件；有界 ingress 与 bootstrap buffer 分离；Projection 的读写、reset、publication 注册都在同一个 owner 上串行执行。对外只交付 owning/copy 数据。
- owner 线程成功创建后才发布 started 状态。生产 start 遇到返回失败或异常都回滚所有已启动产品。
- 所有配置产品共享一个首次就绪绝对截止时间；全体 Live/Synchronized 后绑定 gRPC。一个产品未就绪不能放行整体服务。
- metadata 网络阶段与产品首次同步阶段分别有界，首次同步截止时间不因新增产品或观察到迟到的 Live 而重置。
- 运行后的产品故障隔离。每产品恢复先 quiesce 旧 transport，再在 owner 上 reset/rebootstrap；同时最多一个 active transport。
- 计划轮换沿用 23h50m monotonic 策略和 break-before-make；不拼接两条连接的数据。
- 订单簿 session 在需要完整 Projection rebootstrap 前结束；Event session 在实际 source generation 替换时结束，均不跨 generation 拼接。
- SIGINT/SIGTERM 使用 async-safe signal 模型。停止先关闭 admission、结束并取消/drain server handlers，再停止 transport/owner 和销毁产品；不得在 owner 已不存在时等待其确认。

## 发布与服务边界

`SubscribeOrderBook` 只发布 Projection 已 Applied 的数据。`SubscribeEvents` 需要恰好一个 V1 selector；Spot 支持 DIFF_DEPTH / AGG_TRADE / BOOK_TICKER，USD-M 仅 DIFF_DEPTH。深度事件为 PRE_PROJECTION_NORMALIZED，不代表已应用到订单簿。

每产品 order-book resident 与 Event active 各上限 8；各普通队列 64，另有一个 terminal slot。慢消费者按 session 终止。进程级 TryCancel tracker 总上限 48，与产品数无关；注册/取消握手保护 ServerContext 生命周期。昂贵 unary status 并发上限为一，不占 streaming tracker。status 采用已有 runtime/recovery/publication 观察，没有第二健康框架。

`SubscribeMarketState` 尚未实现。服务直接使用 insecure gRPC，部署在 loopback/可信私网或已有认证保护的代理之后。公开网络认证/TLS 如果成为实际部署需求，再增加专门交付项。

## 构建与历史

Foundation 是独立、无网络无线程的历史最小配置/生命周期 seam；生产目标由 `BMD_GATEWAY_BUILD_PRODUCTION_DAEMON=ON` 启用。Contracts message-only 依赖与单独 gRPC artifact 的选择继续显式保留；G1 冻结 upstream smoke 不随 main 漂移而重新固定。

依赖不使用 floating FetchContent。Gateway 的 offline runtime 测试和 sanitizer 图已经可以本机构建；GitHub 现有默认 CI 只覆盖 Foundation，补生产 CI 是 M3 的要求。

旧 fixed-two 的 recovery 和 performance 记录保留为历史事实。当前配置生产代码的完成，不等同于四产品真实验收或容量证明。性能改动必须由实际瓶颈和同条件前后测量支持，不引入 speculative lock-free、busy polling、affinity、自定义 allocator 或 worker framework。
