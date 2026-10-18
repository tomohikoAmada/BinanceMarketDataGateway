# Gateway G2 v0.3 — 2026-10-18

状态：进行中。用户已授权四产品生产路径集成和最小 production CI；已通读同级
Milestone Plan 六份文档。基线 `ec7071d`（main）；不重做历史 G1，不更新上游 pin。

- [x] 核对现有配置、metadata、owner set、transport/parser、gRPC 和 CI 缺口。
- [x] 四产品 JSON→metadata→spec→独立 parser/Projection→Book/Event/Status 离线组合。
- [x] 产品故障/恢复与慢消费者隔离、全局 48 contexts、启动失败/异常回滚、活动 RPC 退出。
- [ ] production Release CI：固定二进制依赖、构建 bmd-gatewayd、核心离线 smoke。
- [ ] 完整离线、必要 sanitizer、格式、新子代理独立 review、PR/CI/合并。

范围：Spot BTCUSDT/ETHUSDT、USD-M BTCUSDT/ETHUSDT。离线网络边界使用已有
MetadataSources / RecoveryTestOptions / GrpcServiceOptions；不增加生产配置开关。
G3 的真实 Binance/VPS、资源/延迟预算和 BMD-RL-01 联合预算/观察仍未完成。

## 当前已验证（2026-10-18）

- Release production 全图含真实 daemon：23/23，通过；本机日志
  `build/v03-g1/g2-ctest.log`，14.57s。
- 四产品 JSON 使用 examples/gateway.json；每市场只 fetch 一次 fixture metadata，
  各产品不同价格/数量 scale，实际市场 parser、独立 owner/Projection、loopback 三 RPC。
  外部 HTTP/WS 用已有 recovery attempt factory 替换；这不是 TLS/真实行情验收。
- 两种慢消费者使用生产 64 队列，快订阅及其他三产品仍持续收取；USD-M ETH 错 pu
  为 pre-Projection Event，Book 不发布错误更新，旧 session Gap 后结束，新 generation
  单产品重启；其他三产品 generation/freshness 不受影响。
- 四产品合计 48 个真实 streaming RPC、49th RESOURCE_EXHAUSTED；取消释放后可再用。
  每一个产品首次失败及 owner 创建异常全回滚；第四产品未 Live 不绑定 gRPC。
- 真实 signal fixture 由配置/metadata/spec 构造四产品；SIGINT/SIGTERM 时各产品 Book/Event
  共八个活动 RPC，退出后 contexts/transports/subscriptions=0、owners_joined=yes。
  普通构建忽略历史性能导出环境变量；instrumented 构建仍保留明确的双 BTC 导出限制，
  本轮没有四产品 trace 消费需要，不泛化 exporter。
- 子代理首次 review 的两个 CI 准备 blocker（Contracts 固定源码环境、fresh 输出目录）
  已修复；追加旧 session 的 terminal/无错误 Book/非 deadline 断言。
- GitHub 手动首次准备 `39108471777` 因缺上游 fingerprint 的 pytest 失败，原失败保留；
  已补固定 pytest，重跑中。普通 push 在缓存尚未存在时明确失败，无隐式冷编 fallback。
- GitHub cache 有 branch scope；需合并后 main 手动准备一次并验证 consume-only。
  不以 feature 分支结果代替 main 后续 PR 可用性。

本机完整生产 Release、ASan+UBSan、TSan 均 23/23；强化 terminal 断言后再验
Release 23/23 (6.03s)、ASan+UBSan 23/23 (6.05s)、TSan 23/23 (30.58s)。
新测及信号进程测试各本机重复十次，独立 reviewer 各重复二十次；均通过。
Foundation 独立 1/1，格式通过。日志 `build/v03-g2/`（初轮 Release 日志在 `build/v03-g1/`）。
后续仅补可选 instrumented 编译模式的 preflight 测试，未更改 runtime 行为。

## 最终独立复核与可选构建

新子代理只读复核当前分支及最终两项测试增量，无剩余 actionable findings。
已修复其三个发现：固定 Contracts exported recipe 源码 SHA、fresh CI 输出目录、
旧 session 不能把错误 Book 或客户端超时当正常结束。缓存 main scope 单独验收。

可选 instrumented production 的四项重点回归 4/4（6.13s）：四产品直接组合、
production/preflight、历史性能 baseline、进程退出。普通构建四产品八个活动 RPC；
instrumented 进程 fixture 维持历史双 BTC，因为 exporter 本轮未泛化。
默认生产构建不读取该历史环境变量；没有新增配置、线程、Wire 或上游 pin。

GitHub 首次准备的实测耗时主要在 gRPC 编译；本机没有 Linux 固定包，Conan 的兼容
GNU17 查询同样缺少 gRPC，不更改已选运行组合。手动准备增加有限保留的 Conan binary
archive：主分支初始化从同一依赖 key 的成功 run 导入后执行 --build=never 验证，
避免为 GitHub branch scope 再编一次。普通 PR 仍无 source fallback。准备限时调整为
180 分钟，普通生产图 CI 仍为 15 分钟；正在运行的首次任务不因提交而重启。

首次 60 分钟配置的准备任务 `39119072225` 在确认双 gRPC 编译后主动取消，原记录保留；
长限时过渡任务 `39120460290` 在工具 profile 收敛后取消。对 Conan Center 的确切包清单
进一步查询发现 codegen 通用 GNU17 binary 可用，使用独立 `production-ci-build`，只调整
build context 工具标准/插件打包选项；运行侧全部 C++20 设置、版本/RREV、package IDs
保持相同。预解析确认为 build grpc `469bc9791dc5bc8f188c552f6591bf26a02c8822` Download，
build protobuf `f5d75c5ca3a4d6147cbb99a5f8f5d03d2a2471a1` Download；host grpc 仍为原
`033609c8701f6294de7cdccb75ad53e5e4fa4842`。避免源码准备第二份完整 gRPC，无版本 repin。
