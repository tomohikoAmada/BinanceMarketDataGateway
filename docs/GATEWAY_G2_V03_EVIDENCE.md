# Gateway G2 v0.3 — 2026-10-18

状态：完成。四产品离线验证、最小生产 CI、独立复核、PR 合并及 main/PR 缓存消费均已通过。
用户授权本阶段；已通读同级 Milestone Plan 全部六份文档。基线 `ec7071d`。
[PR #37](https://github.com/tomohikoAmada/BinanceMarketDataGateway/pull/37) 已于
2026-10-18 11:30:11 UTC 合并为 `460a85f`，文件内容与受测候选 `24a181e` 相同。

- [x] 四产品 JSON→metadata→spec→parser/私有 Projection→Book/Event/Status 离线组合。
- [x] 单产品恢复、慢消费者、全局 48 contexts、启动失败/异常回滚、活动 RPC 退出。
- [x] 完整离线、必要 sanitizer、格式及新子代理独立 code review。
- [x] 固定二进制 production Release CI 构建真实 bmd-gatewayd，Linux 21/21。
- [x] main 二进制缓存导入、普通 main/PR consume-only 检查及最终文档关闭。

## 四产品生产路径证据

`examples/gateway.json` 配置 Spot BTCUSDT/ETHUSDT 和 USD-M BTCUSDT/ETHUSDT。
每市场一次 fixture metadata 获取，逐产品不同价格/数量 scale，实际 route builder、
Spot combined / USD-M depth / snapshot parser、四个独立 owner/Projection/recovery 和
真实 loopback Book/Event/Status RPC。外部 HTTP/WS 由已有 MetadataSources /
RecoveryTestOptions.attempt_factory 替换；没有生产验收开关，不构成 TLS/真实行情验收。

两种不消费的 publication session 使用生产 64 队列；同产品快 RPC 与其他三产品持续收取。
USD-M ETH 错误 pu 仅出现在 pre-Projection Event，Book 不发布错误更新；旧 session
明确以 Gap 结束且不能靠客户端 deadline 通过。只有该产品进入 generation 2，其他三产品
generation/freshness 不变，新 session 可继续 Applied 更新。

四产品共 48 个真实 streaming contexts，49th 返回 RESOURCE_EXHAUSTED；取消后槽位可复用，
Status 仍可调用。每一个产品首次失败及 owner 创建异常均全部回滚；第四产品未 Live
不能绑定 gRPC。真实 signal fixture 使用四产品配置/metadata/spec，SIGINT/SIGTERM 时
八个活动 Book/Event RPC 被 drain，contexts/transports/subscriptions=0、owners_joined=yes。

普通构建不再读取历史性能导出环境变量，因此四产品不会被遗留变量拒绝。
instrumented 构建保留历史双 BTC exporter 限制和独立回归；G2 无四产品 trace 消费需要，
没有泛化 exporter。未改 Wire、上游 pin、Contracts/Projection/Recorder 源码或 frozen G1 smoke。

## 验证与独立复核

本机 macOS arm64 / AppleClang 21，CMake 4.3.2，复用固定 Conan 二进制：

| 检查 | 结果 |
| --- | --- |
| 完整 production Release（另含历史 synthetic/G8） | 23/23；最终全图 14.34s |
| 完整 production ASan+UBSan | 23/23，6.05s；后续 preflight 增量 2/2 |
| 完整 production TSan | 23/23，30.58s；后续 preflight 增量 2/2 |
| 可选 instrumented production 重点回归 | 4/4，6.13s |
| 独立 Foundation | 1/1，0.01s |
| 新集成/信号进程定点重复 | 本机各 10 次；独立 reviewer 各 20 次，全部通过 |
| clang-format / git diff --check | 通过 |
| Linux Ubuntu 24.04 / GCC13 production | 候选 24a181e，21/21，4.41s |

本机完整矩阵覆盖强化后的 terminal 断言。其后 GCC 可移植性小修仅重命名遮蔽变量、
保存 best-effort write 返回值、把测试临时 lookup key 改有名 const lvalue；G9 定点 1/1、
production/config 定点 2/2（0.97s）、格式和独立复核通过，Linux 对最终候选完整验证。
本机日志在 `build/v03-g2/`；上游固定静态库未重新做 sanitizer instrumentation。

新子代理只读复核实现及各最终增量，无剩余 actionable findings。已修复其三个发现：
exported Contracts recipe 固定源码 SHA 环境、fresh CI 输出目录、旧 session 必须断言
无错误 Book 且非客户端超时结束。未增加通用框架或放宽生产约束。

## 最小生产 CI 与交付

普通 push/PR 只恢复 lock/profile/script 对应的二进制缓存，执行
`--build=never --no-remote`，显式启用 production graph，构建真实 daemon 并运行 21 项
离线测试。不访问 Binance，不重跑 frozen G1，不隐式冷编依赖；Foundation 原检查保留。
手动准备入口才允许从固定 Contracts `518880b` / Projection `555dd1f` 构建缺失包。

Host/runtime 保持 C++20 和原版本/RREV/package IDs。独立 native build profile 选择同版本
GNU17 codegen 二进制：build grpc `469bc9791dc5bc8f188c552f6591bf26a02c8822`、
protobuf `f5d75c5ca3a4d6147cbb99a5f8f5d03d2a2471a1`；host grpc 保持
`033609c8701f6294de7cdccb75ad53e5e4fa4842`。运行侧未 repin。

GitHub cache 有 branch scope，初次 PR 无法读取 feature cache；先用精确候选 push 验证，
合并后导入同 key 的 binary archive 初始化 main，再复验普通 main/PR 路径。
准备 job `39120599182/117655242817` 成功；整个 run 后续 GCC 告警失败，不能记为整轮 PASS。
归档 artifact `12134238442`（170549489 bytes），digest
`sha256:49eb8c1b16bce8001ce326e30c814e6d9456490a365b10d9652c2940c74986eb`。
依赖 key hash：`e42150f1dd5accb898b3333702e73535e7e5c9460e9655679d4130e3665685c1`。

| GitHub 验证 | 结果 |
| --- | --- |
| [候选 push 39123241168](https://github.com/tomohikoAmada/BinanceMarketDataGateway/actions/runs/39123241168) | 全部 7 个普通 jobs 通过，prepare job 按设计跳过；生产 21/21 |
| [main 归档导入 39123505479](https://github.com/tomohikoAmada/BinanceMarketDataGateway/actions/runs/39123505479) | 全绿；input binary_seed_run=39120599182；准备源码步骤跳过，binary-only 验证及生产 21/21（4.39s） |
| [普通 main push 39123491670](https://github.com/tomohikoAmada/BinanceMarketDataGateway/actions/runs/39123491670/attempts/2) | attempt 2 全绿；恢复 main 缓存、离线解析、构建及生产 21/21（4.48s） |
| [原 PR ref 39123245089](https://github.com/tomohikoAmada/BinanceMarketDataGateway/actions/runs/39123245089/attempts/2) | attempt 2 全绿；普通 PR 同样读取 main 缓存，生产 21/21（4.28s） |

main cache ID `9117943881`，ref `refs/heads/main`，大小 160571316 bytes。两次普通运行的
手动准备 job 均跳过；不依赖 feature branch 的缓存或源代码重建。

原失败与取消记录保留：`39108471777` 缺 pytest（已固定）；`39119072225` /
`39120460290` 在双 gRPC 编译与工具 profile 调整时取消；`39120599182`、
`39122712108`、`39122948832` 依次暴露历史 GCC shadow、write 结果和临时 key 告警，
均以局部修正解决，未禁用告警。初始化 main 前的 PR/main cache miss 没有源码 fallback。

G3 未启动：真实 Binance/VPS、四产品资源/延迟预算、BMD-RL-01 联合 API 预算与观察
仍未验收。历史双 BTC 性能和 1..8 资源上限不代表四/八产品容量证明。
