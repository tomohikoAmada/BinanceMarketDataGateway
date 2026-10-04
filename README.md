# Binance Market Data Gateway

C++20 币安实时行情网关。通过启动配置选择多个 Spot / USD-M 永续交易对，获取行情、维护 Projection 订单簿、处理恢复与连接轮换，并通过 gRPC 向下游提供数据。Gateway 不负责历史存储，也不依赖 Recorder。

当前支持 1..8 个精确产品，每产品独立 transport/owner/Projection/recovery，共享一个 gRPC 服务。自选交易对的生产入口已经实现；完整四产品离线验收、生产 CI 和真实网络交付仍按 [milestone plan](docs/MILESTONES.md) 推进。

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

全部配置产品首次同步成功后才开始服务；启动失败会回滚，运行后单个产品故障独立恢复。SIGINT/SIGTERM 会停止订阅和 transport，并清理 owner。进程流式 context 总上限为 48。默认 endpoint 是 loopback，公网部署应使用已有认证保护边界。

提供 `SubscribeOrderBook`、`SubscribeEvents` 和 `GetGatewayStatus`。Spot 事件为 DIFF_DEPTH / AGG_TRADE / BOOK_TICKER，USD-M 事件仅 DIFF_DEPTH。`SubscribeMarketState` 尚未实现。

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

依赖配方见 [conanfile.py](conanfile.py)。完整的无开发机缓存构建/CI 是 M3/M4 的交付项，不能把 Foundation 默认构建当作生产构建成功。ASan、UBSan、TSan 可分别启用 `BMD_GATEWAY_ENABLE_ASAN` / `BMD_GATEWAY_ENABLE_UBSAN` / `BMD_GATEWAY_ENABLE_TSAN`。

保留的双 BTC 生命周期验收客户端须使用双 BTC 配置，与 daemon 使用同一个文件；它不证明四产品验收：

```sh
bmd-gateway-production-acceptance-client \
  --daemon /absolute/path/to/bmd-gatewayd \
  --config /absolute/path/to/two-btc-products.json \
  --grpc-target 127.0.0.1:50051
```

[当前状态](docs/CURRENT_STATE.md) · [开发计划](docs/MILESTONES.md) · [架构](ARCHITECTURE.md) · [代码审查](docs/CODE_REVIEW_2026-10-04.md)
