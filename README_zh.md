# MarketStore
[![GoDoc](http://img.shields.io/badge/godoc-reference-blue.svg)](https://godoc.org/github.com/alpacahq/marketstore) [![chatroom icon](https://patrolavia.github.io/telegram-badge/chat.png)](https://t.me/joinchat/HKxN3BGm6mE5YBt79CMM3Q)


[日本語(Japanese)](README.ja.md)で読む

## 简介
MarketStore是一个专为金融时间序列数据优化的数据库服务器。
您可以将其视为一个可扩展的DataFrame服务，可以从系统的任何地方访问，具有更高的可扩展性。

它从零开始设计，旨在解决处理大量金融市场数据的可扩展性问题，这些数据用于算法交易回测、图表绘制和分析跨越多年的价格历史，粒度可精确到所有美国股票或爆炸性增长的加密货币空间的tick级别。如果您正在为管理大量HDF5文件而苦恼，这是解决您问题的完美方案。

基本安装包含所有必需组件 - 您可以通过简单的[插件](#插件)配置开始从[GDAX](https://docs.gdax.com/#get-historic-rates)拉取加密货币价格数据并将其写入数据库。

MarketStore使您能够通过网络查询DataFrame内容，延迟与从磁盘读取本地HDF5文件一样低，向末尾追加新数据的速度比DataFrame快两个数量级。这是因为存储格式针对数据类型和用例以及现代文件系统/硬件特性进行了优化。

MarketStore已准备就绪！在[Alpaca](https://alpaca.markets)，它已在生产环境中使用了多年，用于严肃的业务。如果您遇到错误或有意参与，请参阅[贡献部分](#开发)了解更多详情。

## 安装

### Docker
如果您想立即开始，可以使用我们最新的[docker镜像](https://hub.docker.com/r/alpacamarkets/marketstore/tags/)引导marketstore数据库实例。该镜像预加载了默认的mkts.yml文件，并声明了VOLUME `/data`作为其根目录。要使用默认设置运行容器：
```sh
docker run -i -p 5993:5993 alpacamarkets/marketstore:latest
```

如果您想运行自定义的`mkts.yml`，可以创建一个新容器并将您的mkts.yml文件加载到其中：
```sh
docker create --name mktsdb -p 5993:5993 alpacamarkets/marketstore:latest
docker cp mkts.yml mktsdb:/etc/mkts.yml
docker start -i mktsdb
```

您也可以[绑定挂载](https://docs.docker.com/storage/bind-mounts/)
容器到本地主机配置文件：自定义的`mkts.yml`：
```sh
docker run -v /full/path/to/mkts.yml:/etc/mkts.yml -i -p 5993:5993 alpacamarkets/marketstore:latest
```
这允许您轻松测试[包含的插件](https://github.com/alpacahq/marketstore/tree/master/plugins#included)，如果您想跳过上面建议的复制步骤。

默认情况下，容器不会将任何写入的数据持久化到容器的主机存储。要实现这一点，请将`data`目录绑定到本地位置：
```sh
docker run -v "/path/to/store/data:/data" -i -p 5993:5993 alpacamarkets/marketstore:latest
```
一旦数据写入服务器，您应该看到如下文件树布局，它将在容器运行之间持续存在：
```sh
>>> tree /<path_to_data>/marketstore
/<path_to_data>/marketstore
├── category_name
├── WALFile.1590868038674814776.walfile
├── SYMBOL_1
├── SYMBOL_2
├── SYMBOL_3
```

如果您已经在本地构建了[cmd](https://github.com/alpacahq/marketstore/tree/master/cmd)包，您可以使用以下命令与正在运行的docker实例建立会话：
```sh
marketstore connect --url localhost:5993
```

### 源码
MarketStore是用Go实现的，所以您可以通过`go install`安装它。
```sh
go install github.com/alpacahq/marketstore/v4@latest
# export GOROOT=$HOME/go
# export PATH=$PATH:$GOROOT/bin
marketstore --version
```

您也可以轻松地从源码构建它。
您需要Go 1.11+，因为它使用`go mod`来管理依赖项。
``` sh
go get -u github.com/alpacahq/marketstore
```
然后使用以下命令编译并安装项目二进制文件：
``` sh
make install
```
可选地，您可以使用以下命令安装项目包含的插件：
``` sh
make plugins
```

### macOS上的Homebrew

您也可以使用[Homebrew](https://brew.sh)包管理器在macOS上安装marketstore。

```sh
$ brew tap zjhmale/marketstore
$ brew install --HEAD marketstore
```

将来要升级marketstore，请使用`upgrade`而不是`install`。

然后您还配备了marketstore服务plist

```sh
$ brew services start marketstore
$ brew services stop marketstore
```

## 使用方法
您可以通过运行以下命令列出可用命令：
```
marketstore
```
或
```
$GOPATH/bin/marketstore
```
取决于您的GOPATH。

您可以通过运行以下命令创建一个名为`mkts.yml`的新配置文件，并填充默认值：
```
$GOPATH/bin/marketstore init
```
然后使用以下命令启动marketstore服务器：
```
$GOPATH/bin/marketstore start
```

输出将类似于：
```
example@alpaca:~/go/bin/src/github.com/alpacahq/marketstore$ marketstore
I0619 16:29:30.102101    7835 log.go:14] Disabling "enable_last_known" feature until it is fixed...
I0619 16:29:30.102980    7835 log.go:14] Initializing MarketStore...
I0619 16:29:30.103092    7835 log.go:14] WAL Setup: initCatalog true, initWALCache true, backgroundSync true, WALBypass false:
I0619 16:29:30.103179    7835 log.go:14] Root Directory: /example/go/bin/src/github.com/alpacahq/marketstore/project/data/mktsdb
I0619 16:29:30.144461    7835 log.go:14] My WALFILE: WALFile.1529450970104303654.walfile
I0619 16:29:30.144486    7835 log.go:14] Found a WALFILE: WALFile.1529450306968096708.walfile, entering replay...
I0619 16:29:30.244778    7835 log.go:14] Beginning WAL Replay
I0619 16:29:30.244861    7835 log.go:14] Partial Read
I0619 16:29:30.244882    7835 log.go:14] Entering replay of TGData
I0619 16:29:30.244903    7835 log.go:14] Replay of WAL file /example/go/bin/src/github.com/alpacahq/marketstore/project/data/mktsdb/WALFile.1529450306968096708.walfile finished
I0619 16:29:30.289401    7835 log.go:14] Finished replay of TGData
I0619 16:29:30.340760    7835 log.go:14] Launching rpc data server...
I0619 16:29:30.340792    7835 log.go:14] Initializing websocket...
I0619 16:29:30.340814    7835 plugins.go:14] InitializeTriggers
I0619 16:29:30.340824    7835 plugins.go:42] InitializeBgWorkers
```

## 配置
为了运行MarketStore，需要一个YAML配置文件。可以使用`marketstore init`创建默认文件(mkts.yml)。此文件的路径通过`start`命令的`--config`标志传入，或者默认情况下它在运行目录中查找名为mkts.yml的文件。

### 选项
变量 | 类型 | 描述
--- | --- | ---
root_directory | string | 允许用户指定MarketStore数据库所在的目录
listen_port | int | MarketStore将通过JSON-RPC API服务的端口
grpc_listen_port | int | MarketStore将通过GRPC API服务的端口
timezone | string | 按TZ数据库名称的系统时区（例如America/New_York）
log_level | string  | 允许用户指定日志级别（info | warning | error）
stop_grace_period | int | 设置MarketStore在收到SIGINT信号后等待关闭的时间量
wal_rotate_interval | int | WAL文件在刷新到磁盘后被修剪的频率（以分钟为单位）
stale_threshold | int | MarketStore将声明符号过期的阈值（以天为单位）
disable_variable_compression | bool | 禁用可变数据的默认压缩
triggers | slice | 触发器插件列表
bgworkers | slice | 后台工作插件列表

### 默认mkts.yml
```yml
root_directory: data
listen_port: 5993
grpc_listen_port: 5995
log_level: info
stop_grace_period: 0
wal_rotate_interval: 5
stale_threshold: 5
```


## 客户端
在您的机器上启动MarketStore实例后，您就可以读取和写入tick数据了。

### Python
[pymarketstore](https://github.com/alpacahq/pymarketstore)是标准的python客户端。确保在另一个终端中，您有marketstore正在运行
* 查询数据
```python
import pymarketstore as pymkts
param = pymkts.Params('BTC', '1Min', 'OHLCV', limit=10)
cli = pymkts.Client()
reply = cli.query(param)
reply.first().df()
```
显示
```python
Out[5]:
                               Open      High       Low     Close     Volume
Epoch
2018-01-17 17:19:00+00:00  10400.00  10400.25  10315.00  10337.25   7.772154
2018-01-17 17:20:00+00:00  10328.22  10359.00  10328.22  10337.00  14.206040
2018-01-17 17:21:00+00:00  10337.01  10337.01  10180.01  10192.15   7.906481
2018-01-17 17:22:00+00:00  10199.99  10200.00  10129.88  10160.08  28.119562
2018-01-17 17:23:00+00:00  10140.01  10161.00  10115.00  10115.01  11.283704
2018-01-17 17:24:00+00:00  10115.00  10194.99  10102.35  10194.99  10.617131
2018-01-17 17:25:00+00:00  10194.99  10240.00  10194.98  10220.00   8.586766
2018-01-17 17:26:00+00:00  10210.02  10210.02  10101.00  10138.00   6.616969
2018-01-17 17:27:00+00:00  10137.99  10138.00  10108.76  10124.94   9.962978
2018-01-17 17:28:00+00:00  10124.95  10142.39  10124.94  10142.39   2.262249
```
* 写入数据
```python
import numpy as np
import pandas as pd
data = np.array([(pd.Timestamp('2017-01-01 00:00').value / 10**9, 10.0)], dtype=[('Epoch', 'i8'), ('Ask', 'f4')])
cli.write(data, 'TEST/1Min/Tick')
# Out[10]: {'responses': None}

cli.query(pymkts.Params('TEST', '1Min', 'Tick')).first().df()
```
显示
```python
                            Ask
Epoch
2017-01-01 00:00:00+00:00  10.0

```

* 可变长度记录
  
Marketstore是一个通过限制时间框架中的记录数量来实现高性能的数据库。
支持的时间框架从`1D`（1天）到`1Sec`（1秒），基本上，
时间框架越长，读取和写入数据的速度就越快。

但是，它也支持不按特定间隔到达或频率超过每秒一次的数据，
如盘口数据和TICK数据。这种数据在marketstore中称为可变长度记录。

您可以通过在pymarketstore写入时指定`isvariablelength=True`来使用可变长度记录功能。

```python
import numpy as np, pandas as pd, pymarketstore as pymkts

symbol, timeframe, attribute_group = "TEST", "1Sec", "Tick"
data_type = [('Epoch', 'i8'), ('Bid', 'f4'), ('Ask', 'f4'), ('Nanoseconds', 'i4')]
tbk = "{}/{}/{}".format(symbol, timeframe, attribute_group)
client = pymkts.Client()

# --- 写入可变长度记录（=单个时间框架（1Sec）中的多个记录）
data = np.array([
    (pd.Timestamp('2021-01-01 00:00:00').value / 10 ** 9, 10.0, 20.0, 1000000),
    (pd.Timestamp('2021-01-01 00:00:00').value / 10 ** 9, 30.0, 40.0, 2000000),
    (pd.Timestamp('2021-01-01 00:00:00').value / 10 ** 9, 50.0, 60.0, 3000000),
], dtype=data_type)
client.write(data, tbk, isvariablelength=True)

# --- 查询可变长度记录
params = pymkts.Params(symbol, timeframe, attribute_group)
print(client.query(params=params).first().df())

# --- 清理
client.destroy(tbk)
```
显示
```
                            Bid Ask Nanoseconds
Epoch                                             
2021-01-01 00:00:00+00:00 10.0 20.0 1000000
2021-01-01 00:00:00+00:00 30.0 40.0 2000000
2021-01-01 00:00:00+00:00 50.0 60.0 3000000
```

因为`Epoch`列的数据类型总是设置为'i8'（=int64），
它不足以描述具有亚秒精度的日期，
亚秒信息存储在marketstore中的另一列（=`Nanoseconds`列，数据类型为'i4'）中。

### 命令行
使用以下命令连接到marketstore实例：
```
// 对于本地数据库-
marketstore connect --dir <path>
// 对于服务器-
marketstore connect --url <address>
```
并通过sql会话运行命令。

## 插件
Go插件架构在linux上的Go1.10+上效果最佳。有关插件的更多信息，请参阅[插件包](./plugins/) 这里介绍了一些特色插件 -

### 流式传输
您可以通过WebSocket流式传输功能接收实时K线更新。数据库服务器在`/ws`上接受WebSocket连接，我们构建了一个推送数据的插件。查看[包](./contrib/stream/)了解更多详情。

### GDAX数据源
包含所有必需组件，因此您可以在安装MarketStore后立即开始从[GDAX](https://docs.gdax.com/#get-historic-rates)拉取加密货币价格数据。然后您可以通过网络查询DataFrame内容，延迟与从磁盘读取本地HDF5文件一样低，向末尾追加新数据的速度比DataFrame快两个数量级。这是因为存储格式针对数据类型和用例以及现代文件系统/硬件特性进行了优化。

如果您配置了数据轮询器，就可以开始从GDAX拉取数据。
有关更多信息，请参阅[包](./contrib/gdaxfeeder/)

### 磁盘聚合
此插件允许您只担心写入tick/分钟级别数据。此插件处理基于时间的磁盘聚合。有关更多信息，请参阅[包](./contrib/ondiskagg/)


## 复制
您可以将数据从主marketstore实例复制到其他marketstore实例。
在`mkts.yml`配置文件中，请按以下方式设置配置：

- 主实例
```
replication:
  # 当enabled=true时，此实例作为主实例工作并接受来自副本的连接
  enabled: true
  # 当tls_enabled=true时，启用主实例和副本之间的传输安全。
  # tls_enabled: true # 主实例和副本都应该有tls_enabled=true来启用TLS
  # 来自一对文件的公钥/私钥对。文件必须包含PEM编码的数据。
  # 证书文件可能包含中间证书，跟随叶子证书形成证书链。
  # cert_file: "/Users/dakimura/projects/misks/tmpcert/server.crt" # 主实例和副本都应该有此配置来启用TLS
  # key_file: "/Users/dakimura/projects/misks/tmpcert/server.key"
  # 用于复制协议的端口
  listen_port: 5996
```

- 副本实例
```
replication:
  # 当设置master_host时，此实例作为副本实例工作
  master_host: "127.0.0.1:5995"
  # 当主服务器上tls_enabled=true时，主实例和副本之间的GRPC通信通过SSL加密。
  # tls_enabled: true
  # cert_file: "/Users/dakimura/projects/misks/tmpcert/server.crt" # 主实例和副本都应该有此配置来启用TLS

```

### 限制
- 目前，复制连接仅在marketstore副本实例启动时初始化。
当您想要复制数据时，请确保首先启动主实例。

- 目前，仅支持`write` API。`delete` API结果不会反映到副本实例。

- 当在副本实例上启用复制时，该实例设置为只读模式，对该实例的write API调用将失败。

## 开发
如果您有兴趣改进MarketStore，我们非常欢迎！只需在GitHub上提交问题或请求，或联系oss@alpaca.markets。在打开PR之前，请确保测试通过-

``` sh
make unittest
```

### 插件开发
我们知道这个空间的需求和要求是多样化的。MarketStore提供强大的核心功能和灵活的插件架构。
如果您想构建自己的插件，请查看[插件](./plugins/)

```go
// 示例插件代码
package main

import (
    "github.com/alpacahq/marketstore/v4/plugins/bgworker"
)

type MyWorker struct {
    config map[string]interface{}
}

func NewBgWorker(config map[string]interface{}) (bgworker.BgWorker, error) {
    return &MyWorker{config: config}, nil
}

func (w *MyWorker) Run() {
    // 您的后台工作逻辑
}
``` 