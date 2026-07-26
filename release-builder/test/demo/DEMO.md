# e2b 兼容沙箱主机 — 端到端演示

`demo_e2b.sh` 用**未改造的 e2b Python SDK**(`pip install e2b e2b-code-interpreter`)把
`orchestrator` 的全链路跑一遍:`Template().from_image()` 构建模板(**拉取 + 展平在构建沙箱
microVM 内进行,客户端不再 docker build/push**)→ 启动真实 microVM → guest 内执行命令 →
**端口转发 + 出网** → 暂停/恢复 → **暂停态转模板扇出** → **一步迁移**(import+resume)→ 销毁。
SDK 零修改,仅靠环境变量 + 本机 `/etc/hosts` + 自签 TLS(`SSL_CERT_FILE`)指向本节点
(与指向 e2b.dev 的方式一致)。

**存储层是持久化前置**:内容存储(store-ctl)、本地 L1 缓存(cache-ctl,tiered rocksdb)、镜像仓库
三者由 `demo_prep.sh` **起一次、常驻复用**(store/cache 监听 **UNIX socket**、不占端口;数据落
`DEMO_DATA_DIR`、跨多次演示复用并缓存镜像与 chunk)。`demo_e2b.sh` 每次只起编排 + eBPF 交换机。

## 演示了什么

| 步骤 | 命令(真实 e2b Python SDK / node-ctl) | 证明 |
|---|---|---|
| 1 | (编排起栈) | orchestrator(TLS) + eBPF vswitch + host NAT;store/cache 经 UDS 复用 |
| 2 | `e2b-key-ctl` + `manifest-key add` | 凭据模型:ManifestKey 保护内容与拉取令牌,固定 KDF 派生缺省 APISecret,APISecret 签发 api_key,两者成对白名单入库 |
| 3 | `Template().from_image(ref).build()` | **构建沙箱**(microVM)内拉取该镜像(租户凭据、租户网络)+ 展平为模板;**无客户端 docker** |
| 4 | `Sandbox.create(template)` | 从模板冷启真实 cloud-hypervisor microVM,guest 内 envd 就绪 |
| 5 | `sbx.commands.run(…)` | guest 内执行命令(默认用户 `user`,可写 `/home/user`)经 proxy→envd |
| 6 | `curl http://<floatingip>:port` / `https://<port>-<sid>.<domain>` | **端口转发** + **沙箱出网**(NAT) |
| 7 | `sbx.pause()` / `Sandbox.connect(id)` | 快照入内容存储 → 恢复(**resume == connect**);暂停前写入恢复后仍在 |
| 8 | `export-sandbox --to-template` → `Sandbox.create(<tmpl>)` | 暂停态晋升为远程模板、扇出**新**沙箱(带 forked 状态) |
| 9 | `export-sandbox` → `Sandbox.connect(id, api_headers={migration-token})` | **一步迁移**:connect 自动 import + resume |
| 10 | `Sandbox.list()` / `sbx.kill()` | 生命周期 |

## 前置条件

- 二进制(源码树中执行 `make -C orchestrator/release-builder build`;release 包内已自带):`node-ctl`、`store-ctl`、**`cache-ctl`**(CGO/rocksdb)、
  `flatten-ctl`、`e2b-key-ctl`、`connector-ctl vswitch`、`cloud-hypervisor`、`vmlinux`、`sandbox-runtime.erofs`。
- 主机:**systemd 为 PID1 + root**(编排经 D-Bus 驱动单元;TLS :443;KVM);可读写 `/dev/kvm`。
- **e2b Python SDK**:`pip install e2b e2b-code-interpreter`。
- 工具:`python3`、`openssl`、`iproute2(ip)`、`curl`、`sqlite3`、`iptables`;`demo_prep.sh` 另需 `docker`(一次性把
  base 镜像 seed 进仓库)和(无第三方仓库时)`zot`。
- **镜像仓库**:`REGISTRY=<host:port>`(+ `REGISTRY_USER`/`REGISTRY_PASS`/`REGISTRY_INSECURE`)指向第三方仓库;
  或不设 `REGISTRY` 让 `demo_prep.sh` 起一个**持久本地 zot**。base 镜像默认 `e2bdev/code-interpreter:latest`
  (`E2E_IMAGE=` 覆盖)。

## 运行

```bash
# 源码树(从 org root 运行)
# ① 一次性 / 持久前置:起 store(UDS) + cache(UDS) + 仓库,seed base 镜像(重复运行幂等、跳过已起的)
REGISTRY=registry.example.com REGISTRY_USER=u REGISTRY_PASS=p  bash orchestrator/release-builder/test/demo/demo_prep.sh
bash orchestrator/release-builder/test/demo/demo_prep.sh            # 或不设 REGISTRY → 起持久本地 zot
#   停服务: demo_prep.sh stop   ·   停并清数据: demo_prep.sh reset

# ② 每次演示(读取 demo_prep 写出的 ~/.cache/kuasar-demo/prep.env)
sudo bash orchestrator/release-builder/test/demo/demo_e2b.sh        # 普通用户也行:会自动 sudo 重入
DEMO_PAUSE=1   sudo bash …/demo_e2b.sh                # 每步回车暂停(适合逐步讲解 / 另开终端操作)
DEMO_KEEP=1    sudo bash …/demo_e2b.sh                # 保留每次工作目录排查
DEMO_NETDIAG=1 sudo bash …/demo_e2b.sh                # 网络步失败不中止

# release 包(从解包目录运行)
bash test/demo/demo_prep.sh
sudo bash test/demo/demo_e2b.sh
```

`demo_e2b.sh` 退出即清理本次的编排单元 / vswitch / NAT 规则 / `/etc/hosts` 临时项 / 工作目录;**持久存储层
(store/cache/仓库)保留**,下次演示直接复用。

## 网络配置(端口转发 + 出网)

e2b profile 的 guest 网卡是一个 link-local **inner IP** `169.254.0.21/30`、默认路由 nexthop `169.254.0.22`
(/30+网关让 envd 端口转发可用;所有 e2b 沙箱相同——唯一身份是 floatingip);host 与外网都不在该网段。
三处配置把"host↔沙箱"与"沙箱→外网"打通——脚本已自动完成:

```
  host (root netns)                       vswitch (netns sw0)                 guest microVM
  curl <floatingip>:port ─route─► sw0m0 ─► ARP-proxy + DNAT floatingip->inner ─tap─► eth0 169.254.0.21/30
  reply ◄───────────────────────── SNAT inner->floatingip ◄──────── tap ◄──────────  http.server :port
                                                                                      (default via 169.254.0.22)

  egress: guest ─(default via 169.254.0.22)─► nx extract 0.0.0.0/0 ─► sw0m0
          ─► host NAT MASQUERADE (-s 100.100.96.0/20, ip_forward=1) ─► internet
```

1. **`connector-ctl vswitch start … --mgmt-extract=:sw0m0:169.254.169.254,0.0.0.0/0`** 在 host(root netns) 建管理网卡
   `sw0m0`、自动加路由 `100.100.96.0/20 dev sw0m0`。eBPF 在 sw0m0 侧 ARP 代答 + 把 host 发往 floatingip 的包
   **DNAT** 成沙箱 inner IP、重定向到对应 tap,回包再 **SNAT** 回 floatingip;`mgmt_cidrs` 含 `0.0.0.0/0` ⇒ 出网入口。
2. **host NAT**(脚本幂等加、退出删):`iptables -A FORWARD -{i,o} sw0m0 …` + `-t nat -A POSTROUTING -s 100.100.96.0/20 -j MASQUERADE`。
3. **guest inner IP + 默认路由 + `/etc/hosts`/`/etc/resolv.conf`** 由 orchestrator 经 SANDBOX_CONFIG `files:` 自动下发
   (hostname 治 getfqdn 卡顿、见 node.md §11;guest DNS `169.254.169.253` 经 demo 的 iptables DNAT 路由到本机首个 nameserver)。

两条访问沙箱端口的路径(步骤 6 均验证):**直连** host 经 `sw0m0` 直达 `http://<floatingip>:<port>`;**e2b 暴露端口**
`https://<port>-<sid>.<domain>`,proxy 校验 `X-Access-Token` 后转发到 `floatingip:port`。

> **per-sid `/etc/hosts` 即可(SDK `create` 是惰性的)**:`Sandbox.create()` 只 POST 控制面、构造对象即返回,**不**
> 急于连数据面(仅 `mcp` 模式才急连);故 demo 在 create 拿到 sid **后**再把 `49983-<sid>.<domain>` 等加进
> `/etc/hosts`,首条命令时才连 envd——无需通配 DNS。若要在另一窗口对**自己**新建的沙箱即时交互,要么照此先加 host,
> 要么配通配 DNS(dnsmasq `address=/<domain>/127.0.0.1`,`/etc/hosts` 不支持通配)。

**另开终端手动操作**:演示在"租户接入"步把 SDK 凭据写入 `/tmp/demo-e2b-cli-env.sh`。配合 `DEMO_PAUSE=1`,另开终端
`source /tmp/demo-e2b-cli-env.sh` 后即可用 SDK 操作本节点(`python3 -c "from e2b import Sandbox; print([s.sandbox_id for s in Sandbox.list()])"`)。

## SDK 如何指向本节点(零改造)

e2b SDK 用 `E2B_DOMAIN` 推出控制面 `https://api.<domain>` 与数据面 `https://<port>-<sid>.<domain>`:

- `E2B_DOMAIN`/`E2B_API_KEY` 指向本节点;**构建直接 `from_image(<registry>/<image>)`**——构建沙箱内拉取并展平
  (凭据来自租户默认或任务级 token,见 node.md §12),**不再有客户端 docker build/push 或 `E2B_IMAGE_URI_MASK`**。
  镜像 ref 须**从构建沙箱可达**:本地 zot 经 vswitch mgmt VIP(`169.254.169.254:<port>`)寻址(脚本自动改写),
  第三方仓库经 NAT 出网直达。
- 自签 `*.<domain>` 证书 + **`SSL_CERT_FILE=<cert>`**(httpx 信任)让 SDK 接受 TLS。
- `/etc/hosts` 把 `api.<domain>` 与每个沙箱的 `49983/49999/<port>-<sid>.<domain>` 解析到 `127.0.0.1`(创建后加、退出删)。

## 说明与注意

- **base 镜像须满足 e2b userland 约定**:有 `user` 账户、`/bin/bash`、`util-linux`/`coreutils`(envd 以默认用户、
  包一层 `ionice … nice …` 执行命令)。`e2bdev/code-interpreter` 本就具备;它由 `demo_prep.sh` seed 进仓库一次,
  模板 `from_image` 直接命名它、构建沙箱内拉取,无任何 shim/改写。
- **envd 以 root 运行**:envd 是 e2b 基础设施,须 root 才能 setuid 到镜像默认用户执行负载命令(e2b profile 固定
  `launch.user=0:0`,不沿用镜像 `Config.User`)。
- **持久存储复用**:`demo_prep.sh` 的 store/cache/仓库常驻、数据落 `DEMO_DATA_DIR`(默认 `~/.cache/kuasar-demo`),
  跨多次演示去重缓存 → 复跑快;`demo_prep.sh reset` 清空重来。
- **自签 TLS** 仅为本机演示;生产用通配 `*.<domain>` 正式证书(见 `orchestrator/docs/node.md` §13)。

自动化回归(断言版、非讲解版)见 `orchestrator/release-builder/test/e2e/`:`e2e_run_builder.sh`(三阶段构建流水线:
guest 内拉取展平 → steps → 模板快照 → 从产物模板 create)与 `e2e_execute.sh`(启动+执行+暂停/恢复状态存活)。
