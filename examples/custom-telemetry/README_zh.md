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

`Configure` 在 store/listener 启动前执行一次。Config 存放声明，Runtime 存放不可序列化
的进程内绑定。本例保留选定的 primary storage，记录扩展启动/停止，并可从
`TELEMETRY_EXPORTER_AUTHORIZATION` 提供 extra exporter 的 Authorization header。
一旦绑定，该 provider 替换 YAML header map；失败不会回退到 YAML 凭据。不要记录或
上传 secret、诊断环境。生产构建可以改用受保护文件或凭据服务。

`collector.go` 单独演示窄 `app/telemetry/otel` API：真正的 Collector processor 添加
部署属性并保留 ingress context。core 在定制 processor 前后分别放置 identity enrichment
与最终 guard。不要丢弃 context、把不同 sandbox 合成一个 resource、替换可信属性，
或在 guard 之前插入其他 receiver。进入 primary storage 和各 exporter 前，core 拒绝
缺失/过期身份并覆盖 SandboxID/StableID；RunID 不进入指标身份。

定制主存储使用 `storage.type: custom` 并绑定 `Runtime.Storage`，实现
`extension.Storage`：canonical Collector 写入、精确 SandboxID 的 Bounds/Query、
Shutdown。它不是 extra exporter。`Runtime.StorageHeaders` 为内建 Prometheus/
ClickHouse 主存储提供材料。额外 Collector exporter 通过独立
`otel.Components.Exporters` 列表绑定，不自动使历史可查询。普通 `extension` 包没有
OTel 类型，也没有动态 registry 或 DI container。

完整契约见 [telemetry 规范](../../docs/telemetry_zh.md) 和
[扩展指南](../../docs/extensions_zh.md#telemetry-bootstrap-与扩展)。
