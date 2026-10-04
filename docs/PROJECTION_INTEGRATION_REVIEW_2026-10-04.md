# Projection 配套评估 — 2026-10-04

本次评估针对 Gateway 新增的高性能、安全稳定、低延迟、低 CPU/内存消耗目标。只更新 Gateway 计划和说明；未修改 Projection 生产代码、应用其工作区已有 patch、升级依赖或采集新性能数据。

## 结论

Gateway 生产路径已经实际使用 Projection。自选多个交易对不要求新增 Projection API；每个产品使用一个独立实例。为完成质量目标，需要配套做 Projection 性能评估和集成回归；Core 是否需要局部优化尚待负载、预算和 profiling 证据决定。

## 核对版本与调用证据

- Gateway 评估基线：`0b76bc51f1d7071998e00a8f03c1a536b50955b8`，G12-C 配置生产已经合并。
- Projection 本地 main 与 GitHub main 均为 `555dd1f4d846e19cfd6fbe55676c86504559048e`；通过 GitHub commit API 与 `git ls-remote` 核对。网页缓存中的旧 M6 状态不作为当前证据。
- Gateway `conanfile.py` 固定 `binance-market-data-projection/0.1.0#d95fa71d6dca8d931e72fbb5b74114a9`；生产依赖 Core 和 ProtoAdapter，未使用浮动 main。
- `src/g11/multi_market_runtime.cpp` 为每个精确 MarketKey 构造独立 MarketRuntime，并传入该产品 NumericSpec 与 ExpectedIdentity；`src/g3/market_runtime.cpp` 持有私有 `core::BookProjection`。
- owner 中 REST snapshot 经 `adapt_exchange_depth_snapshot(...).install_into(projection_)` 建立 baseline；WebSocket depth 经 `adapt_depth_update(...).apply_to(projection_)` 更新；消费者快照通过 Projection Adapter 构造。不是仅声明 CMake 依赖或只在 smoke 中使用。
- 本机已验证生产图的链接包含 Projection Core/ProtoAdapter 库。其 Conan 构建缓存内 `book_projection.cpp` 与上述 Projection main 文件逐字节一致；下述成本路径存在于实际依赖的构建源。
- Projection 从此前消费的 `8621499cbeba0e42c409572ee3f209c32691698b` 到 current main 的 `src/`、`include/` 没有差异。

调用关系：

```text
Gateway transport / normalized depth
  -> bounded MarketRuntime queue / serialized owner
  -> Projection ProtoAdapter (owning numeric conversion / identity checks)
  -> private BookProjection (Spot/USD-M sequencing / OrderBook mutation)
  -> Projection snapshot adapter / Gateway bounded publication / gRPC
```

Event depth 是 pre-Projection 分支；并非每个原始事件都要经过订单簿处理。Projection 不负责网络、队列线程、恢复调度或 gRPC。

## 为什么多交易对不需要改 Projection API

`ExpectedIdentity` 使用调用方传入的 symbol 和 sequence policy，NumericSpec 也是每个实例输入；没有固定 BTCUSDT allowlist。Core 是单产品、单 writer、无网络的嵌入式库。Gateway 组合多个实例就能隔离不同交易对与精度，Spot 与 USD-M 也已经有对应策略。

## 明确的性能候选

[`BookProjection::apply_transaction`](https://github.com/tomohikoAmada/BinanceMarketDataProjection/blob/555dd1f4d846e19cfd6fbe55676c86504559048e/src/projection_state/book_projection.cpp#L198-L205) 在每个被接受的更新批中：

1. 用 `all_levels` 复制全部 bid 与 ask 到 vector；
2. 建立新的 candidate OrderBook，并 `replace_all` 重建全部 map 节点；
3. 在 candidate 应用本批更新，再 move 提交。

这使成本随现有簿深增长，包含全簿遍历、临时数据和节点分配，即使本批只修改少量档位。它用于保证分配/更新异常发生时原簿及序列状态不变；这是需要保留的安全语义。这里确认的是执行成本，不是已测得的 Gateway 主瓶颈、具体延迟、CPU 占比或容量结论。

过去的 [KEEP_STD_MAP 容器实验](https://github.com/tomohikoAmada/BinanceMarketDataProjection/blob/555dd1f4d846e19cfd6fbe55676c86504559048e/docs/M5_PHASE9_CONTAINER_DECISION.md) 没有接受容器迁移。该历史结果有参考价值；全簿事务复制与容器选择是两个需要分别测量的因素，不能直接推导更换容器会提高完整链路性能。

## 配套迭代顺序

1. M3 准备 Gateway 四产品生产路径与不同簿深/更新批大小/突发负载，复用 Projection benchmark、replay、分配失败测试及 Gateway instrumentation。
2. M4 声明目标机器与持续/突发负载、p99 延迟、CPU/RSS 和吞吐预算；分别测完整链路、Adapter+apply 和 Core 更新/分配，找出影响预算的路径。
3. 若该路径影响预算，优先探索仅准备本批涉及档位的事务方案，减少全簿复制/重建；提交阶段与失败路径仍必须满足现有强异常保证、重复价格语义和序列状态原子性。这里不指定最终算法或新增通用事务框架。
4. 在 Projection 仓库用 differential/replay、异常注入、fuzz/benchmark 验证，再生成并固定新 package revision；Gateway 跑完整四产品集成、sanitizer 与同条件性能比较。只优化 Gateway 不能绕过或复制 Projection 的业务语义。
5. Projection README/CURRENT_STATE/MILESTONES 中的固定双 BTC 与旧 Gateway 下一阶段描述需要更新；该文档工作可单独进行，不应为了文档对齐修改 Core。

没有新增 Projection 多 symbol manager、线程、网络或第二订单簿，也没有把“已有测试通过”当作质量目标达标。数值预算和多产品性能验收目前仍未完成。
