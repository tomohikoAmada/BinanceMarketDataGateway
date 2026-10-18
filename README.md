# Binance Market Data Gateway

以高性能、安全稳定、低延迟、低 CPU/内存消耗为目标的 C++20 币安实时行情网关。通过启动配置选择多个 Spot / USD-M 永续交易对，获取行情、维护 Projection 订单簿、处理恢复与连接轮换，并通过 gRPC 向下游提供数据。Gateway 不负责历史存储，也不依赖 Recorder。

当前支持 1..8 个精确产品，每产品独立 transport/owner/Projection/recovery，共享一个 gRPC 服务。自选交易对的生产入口已经实现；完整四产品离线验收、生产 CI、性能/稳定性预算验收和真实网络交付仍按 [milestone plan](docs/MILESTONES.md) 推进。

## 配置与运行

```json
{
  "grpc_listen": "127.0.0.1:50051",
  "spot_symbols": ["BTCUSDT", "ETHUSDT"],
  "usdm_symbols": ["BTCUSDT", "ETHUSDT"]
}
```

```sh
bmd-gatewayd --config examples/gateway.json
```

可以只配置其中一个市场；总数必须为 1..8，同一 symbol 的现货和永续是不同产品。symbol 使用币安的精确大写标识，不自动修正大小写，且需要通过 exchangeInfo 交易资格与精度验证。配置变更后重启。

全部配置产品首次同步成功后才开始服务；启动失败会回滚，运行后单个产品故障独立恢复。SIGINT/SIGTERM 会停止订阅和 transport，并清理 owner。进程流式 context 总上限为 48。无认证 gRPC 必须使用数值 loopback（IPv4 127/8 或 IPv6 ::1）；hostname、wildcard 和非 loopback 配置被拒绝。

`GetGatewayStatus` 的 owner 观察最多等待一秒，并遵守更短的客户端 deadline；超时返回 DEADLINE_EXCEEDED。新版 v0.3 G0 的修复/离线验证见 [证据](docs/GATEWAY_G0_V03_EVIDENCE.md)。

提供 `SubscribeOrderBook`、`SubscribeEvents` 和 `GetGatewayStatus`。Spot 事件为 DIFF_DEPTH / AGG_TRADE / BOOK_TICKER，USD-M 事件仅 DIFF_DEPTH。`SubscribeMarketState` 尚未实现。

v0.3 G1 起，`GetGatewayStatus.markets[].last_event_utc_ns` 表示当前 generation 最后经 Projection **Applied Depth 的接收 UTC**；重同步、Gap、来源或时间未知时为空，零时间保持 present。该字段从历史“任意 WebSocket 事件时间”收窄，trade/ticker 活跃不会再掩盖停止更新的簿。Live 表示序列同步，不承诺数据年龄；客户端遇墙钟回退时须将年龄视为未知。安静市场不会自动触发 Gap 或重同步。USD-M 深度 `T` 已更正为 transaction-time，trade-time 留空；[G1 证据](docs/GATEWAY_G1_V03_EVIDENCE.md)。

## 构建与验证

独立 Foundation（不包含生产 daemon）：

```sh
cmake --preset gcc-debug
cmake --build --preset gcc-debug
ctest --preset gcc-debug
scripts/format-check.sh
```

生产图需要固定版本 Contracts/Projection 和 Conan 依赖，显式启用：

```sh
cmake -S . -B build/production \
  -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_TOOLCHAIN_FILE=/absolute/path/to/conan_toolchain.cmake \
  -DBMD_GATEWAY_BUILD_PRODUCTION_DAEMON=ON \
  -DBMD_GATEWAY_BUILD_G2_SYNTHETIC_HOST=ON \
  -DBMD_GATEWAY_BUILD_G8_INTEGRATION_ACCEPTANCE=ON \
  -DBMD_GATEWAY_BUILD_TESTS=ON
cmake --build build/production --parallel 4
ctest --test-dir build/production --output-on-failure
```

依赖配方见 [conanfile.py](conanfile.py)，生产依赖 RREV 固定于 [conan.lock](conan.lock)。
GitHub CI 保留独立 Foundation 检查，另用 Ubuntu 24.04 / GCC 13 构建真实 `bmd-gatewayd`
并运行生产图离线回归（包括四产品集成及活动 RPC 信号退出），不访问 Binance。
普通 PR 只恢复二进制缓存并执行 `--build=never --no-remote`；无缓存时明确失败。
首次接入、缓存过期或依赖变更后，在 **main** 手动运行 Gateway CI，勾选
`prepare_dependencies`。该入口才允许从固定 Contracts/Projection SHA 准备缺少的二进制；
缓存按 lock/profile/preparation script 隔离，后续 PR 复用 main 缓存。
从分支首次接入时，在同一分支已有缓存后再手动运行一次以导出二进制归档；合并后
main 准备填写该成功 run 的 `binary_seed_run`，直接导入同一批二进制。归档保留 14 天，
无需为转移 GitHub 分支范围再冷编依赖。依赖变更需先准备对应缓存。
历史 G1 upstream smoke 仍保持单独显式入口，不随本 CI 重复执行。ASan、UBSan、TSan 可分别启用 `BMD_GATEWAY_ENABLE_ASAN` / `BMD_GATEWAY_ENABLE_UBSAN` / `BMD_GATEWAY_ENABLE_TSAN`。

保留的双 BTC 生命周期验收客户端须使用双 BTC 配置，与 daemon 使用同一个文件；它不证明四产品验收：

```sh
bmd-gateway-production-acceptance-client \
  --daemon /absolute/path/to/bmd-gatewayd \
  --config /absolute/path/to/two-btc-products.json \
  --grpc-target 127.0.0.1:50051
```

[当前状态](docs/CURRENT_STATE.md) · [开发计划](docs/MILESTONES.md) · [架构](ARCHITECTURE.md) · [代码审查](docs/CODE_REVIEW_2026-10-04.md)
