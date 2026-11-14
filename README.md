# NPanel-node

A NPanel node server based on xray-core, modified from v2node.  
一个基于xray内核的NPanel节点服务端，修改自v2node

## 单服务器多 IP 出站

公网 IP 直接配置在节点网卡上时，TCP 入站继续监听 `0.0.0.0`，默认直连流量自动使用客户端实际连接的本机 IP 出站，无需为每个 IP 启动一个 npnode。

例如，同一台服务器有 `203.0.113.10`、`203.0.113.11`：客户端连接 `.10` 时，IPv4 直连流量从 `.10` 发出；连接 `.11` 时，从 `.11` 发出。通过 TCP 隧道承载的 UDP 流量也使用对应入口 IP。

- 面板添加多个入口后，还需要更新节点服务器上的 npnode 程序并重启节点服务，仅更新面板不能改变出站行为。
- 附加 IP 必须已经配置在本机，且供应商允许该 IP 对外通信；主机或上游的 SNAT 仍可能改写公网出口。
- 本机回环反代连接访问外网、自定义代理出站及已有源地址设置保留原有行为。
- 源地址绑定适用于同地址族的直连流量；IPv4 入口访问 IPv6 字面地址等跨地址族请求继续由系统选择源地址。
- Hysteria2、TUIC、Shadowsocks 原生 UDP 等通配 UDP 入站不在本次支持范围内：当前内核可能只提供 `0.0.0.0`，无法识别实际入口 IP，不保证源进源出。

可以先在节点服务器测试附加 IP 本身的出站能力，再通过客户端连接该入口，使用同一 IPv4 查询接口验证出口：

```bash
# 将示例 IP 替换为网卡上的实际附加公网 IPv4。
curl -4 --noproxy '*' --interface 203.0.113.11 --connect-timeout 5 --max-time 15 https://api.ipify.org
```

Linux 多入口回归测试通过一个监听 `0.0.0.0` 的真实 VLESS 入站，并发连接四个本机 IP，在目标端检查 TCP 和 UDP-over-TCP 的源地址：

```bash
GOEXPERIMENT=jsonv2 go test ./core -run '^TestVLESSWildcardInboundPreservesEntryIP$' -count=1
```

## 软件安装

### 一键安装

```
wget -N https://raw.githubusercontent.com/yunsen1599/NPnode-release/main/scripts/install.sh && bash install.sh
```

## 构建
``` bash
GOEXPERIMENT=jsonv2 go build -v -o ./npnode -trimpath -ldflags "-s -w -buildid="
```

## 发布

源码仓库是私有的，安装脚本和安装包发布在公开仓库 [yunsen1599/NPnode-release](https://github.com/yunsen1599/NPnode-release)，节点服务器无需任何凭据即可一键安装和更新。

``` bash
scripts/release.sh 1.0.5
```

脚本会编译 linux 64 / arm64-v8a / s390x 安装包，把 `scripts/install.sh`、`scripts/npnode.sh`、`README.md` 同步到发布仓库，创建同名 Release，并在源码仓库打 tag。
