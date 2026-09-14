[English](README.md) | [简体中文](README_zh.md)

# 定制 telemetry

构建受信、静态链接的 App：

```sh
GOWORK=off CGO_ENABLED=0 go build -o /opt/kuasar/bin/custom-telemetry ./examples/custom-telemetry
```

保护 executable，禁止 group/world 写入。root 启动要求 root 属主；非 root 启动接受
root 或服务 UID 属主。在独立 `telemetry.yaml` 的 `paths.telemetry_executable` 设置
绝对路径，然后通过 `node-ctl telemetry serve --config /etc/node-ctl/telemetry.yaml`
启动。直接执行会被拒绝。node-ctl 校验文件、创建 sealed bootstrap 并原地替换进程；
定制启动失败不会回退到内置实现。

`proxy_netns` 配置通过 `Config.ProxyNetNS` 传入 `Configure`.
它选择两种沙箱 OTLP listener 的 namespace, 不移动 executable 或其远端 client.
早期 Preview 的 `sandbox_netns` key 必须改名, strict bootstrap/config decode 拒绝旧名称.
部署方法见 [management 网络](../../docs/telemetry_zh.md#4-直接面向沙箱的-otlp).

`Configure` 在 store/listener 前执行一次. Config 保存声明, Runtime 保存进程内
binding. 示例记录 lifecycle start/stop 并注册 `privatedeployment` processor factory.
Exporter 凭据通过组件选项中的原生 config provider 提供, 如 `${env:OTLP_TOKEN}`;
provider 失败会终止启动, 不会改选其他凭据.
[telemetry.yaml](telemetry.yaml) 是完整 write-only 示例, 包含三个原生 receiver、
定制 processor、batch 和 OTLP HTTP exporter. 将 `OTLP_ENDPOINT` 设置为目标地址.
没有选定 reader 时 `/metrics` 不可用; conductor 的原生 stats 继续工作.

在 `collector.processors` 添加 `privatedeployment: {}`, 并将 `privatedeployment` 加入
需要它的 pipeline `processors` 列表. 注册 factory 本身不会将它插入所有 pipeline.
`collector.go` 展示普通 mutating Collector processor. 来源身份已经接纳到每个 pdata
resource, 标准 batch、queue、retry 可以丢失原请求 context, 也可以组合多个 resource.
不同沙箱身份不能合并进同一 resource. Pause/delete 不丢弃已接纳历史, 用户
`application.run_id` 会保留. 静态 factory 可增加 receiver、processor、exporter、
connector、Collector extension、config provider、converter; 实例与 pipeline 边仍由
声明配置决定.

定制主存储使用 `storage.type: custom` 并绑定 `Runtime.Storage`，实现
`extension.Storage`：canonical Collector 写入、精确 SandboxID 的 Bounds/Query、
Shutdown。它不是 extra exporter。`Runtime.StorageHeaders` 为内建 Prometheus/
ClickHouse 主存储提供材料。额外 Collector exporter 通过独立
`otel.Components.Exporters` 列表绑定，不自动使历史可查询。普通 `extension` 包没有
OTel 类型，也没有动态 registry 或 DI container。

完整契约见 [telemetry 规范](../../docs/telemetry_zh.md) 和
[扩展指南](../../docs/extensions_zh.md#telemetry-bootstrap-与扩展)。
