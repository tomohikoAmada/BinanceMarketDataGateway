# Gateway G2 v0.3 — 2026-10-18

状态：进行中。用户已授权四产品生产路径集成和最小 production CI；已通读同级
Milestone Plan 六份文档。基线 `ec7071d`（main）；不重做历史 G1，不更新上游 pin。

- [x] 核对现有配置、metadata、owner set、transport/parser、gRPC 和 CI 缺口。
- [ ] 四产品 JSON→metadata→spec→独立 parser/Projection→Book/Event/Status 离线组合。
- [ ] 产品故障/恢复与慢消费者隔离、全局 48 contexts、启动失败/异常回滚、活动 RPC 退出。
- [ ] production Release CI：固定二进制依赖、构建 bmd-gatewayd、核心离线 smoke。
- [ ] 完整离线、必要 sanitizer、格式、新子代理独立 review、PR/CI/合并。

范围：Spot BTCUSDT/ETHUSDT、USD-M BTCUSDT/ETHUSDT。离线网络边界使用已有
MetadataSources / RecoveryTestOptions / GrpcServiceOptions；不增加生产配置开关。
G3 的真实 Binance/VPS、资源/延迟预算和 BMD-RL-01 联合预算/观察仍未完成。
