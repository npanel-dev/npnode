# NPanel-node

A NPanel node server based on xray-core, modified from v2node.  
一个基于xray内核的NPanel节点服务端，修改自v2node

## 软件安装

### 一键安装

```
wget -N https://raw.githubusercontent.com/npanel-dev/npnode/main/scripts/install.sh && bash install.sh
```

## 构建
``` bash
GOEXPERIMENT=jsonv2 go build -v -o ./npnode -trimpath -ldflags "-s -w -buildid="
```
