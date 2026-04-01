# GameStorage ( ˶ˊᵕˋ)੭

基于 MongoDB 的游戏通用存储系统。

通过 `Collection + Key + PlayerID` 三元组定位任意数据，所有类型共用一张表，索引数量固定，不随业务增长。

## 特性

- **乐观并发控制** — 写入时对 Value 计算 SHA-256 生成 version，更新时校验 version 一致性，不一致则拒绝写入
- **批量操作 + 排序防死锁** — 写入/删除支持批量，操作前按 `collection → key → ownerID` 排序以固定加锁顺序
- **游标分页** — 基于排序键的游标分页，避免 offset 在大数据量下的性能问题
- **事务重试** — MongoDB 事务自动重试（瞬时错误重试 + 提交阶段重试）
- **Schema-Free** — Value 为 JSON 字符串，字段变更时旧数据自然兼容，无需停服迁移
- **删除幂等** — 不带 version 的删除操作，目标不存在也返回成功

## 安装

```bash
go get github.com/wepec-studio/storage
```

## 快速开始

```go
package main

import (
    "context"
    "go.uber.org/zap"
    "github.com/wepec-studio/storage"
)

func main() {
    ctx := context.Background()
    logger, _ := zap.NewProduction()

    // 连接数据库并创建 Store
    config := storage.NewDefaultDatabaseConfig()
    config.URI = "mongodb://localhost:27017"
    store, err := storage.New(ctx, logger, config)
    if err != nil {
        panic(err)
    }
    defer store.Close(ctx)

    // 写入
    acks, err := store.Write(ctx, []*storage.OpWrite{
        {
            OwnerID: "player_001",
            Object: &storage.WriteObject{
                Collection: "bag",
                Key:        "items",
                Value:      `{"slots":[{"id":"sword_01","count":1}]}`,
            },
        },
    })

    // 读取
    objects, err := store.Read(ctx, []*storage.ReadObjectID{
        {Collection: "bag", Key: "items", PlayerID: "player_001"},
    })

    // 带版本号更新 (乐观锁)
    acks, err = store.Write(ctx, []*storage.OpWrite{
        {
            OwnerID: "player_001",
            Object: &storage.WriteObject{
                Collection: "bag",
                Key:        "items",
                Value:      `{"slots":[{"id":"sword_01","count":2}]}`,
                Version:    objects[0].Version, // 携带当前版本号
            },
        },
    })

    // 列表查询 (游标分页)
    list, err := store.List(ctx, "player_001", "bag", 10, "")
    // list.Cursor 非空时，传入下次查询即可翻页

    // 删除
    err = store.Delete(ctx, []*storage.OpDelete{
        {
            OwnerID:  "player_001",
            ObjectID: &storage.DeleteObjectID{Collection: "bag", Key: "items"},
        },
    })
}
```

## 数据模型

每个存储对象由三个字段唯一标识：

```
Collection + Key + PlayerID → 唯一对象
```

| 字段 | 含义 | 示例 |
|------|------|------|
| Collection | 逻辑分组 | `bag`, `character`, `quest` |
| Key | 集合内的对象键 | `items`, `profile`, `main` |
| PlayerID | 所有者 | `uuid-xxx` |

所有数据存储在同一个 MongoDB Collection 中，通过 `collection` 字段区分逻辑分组。

**存储示例：**

| collection | key | player_id | value |
|---|---|---|---|
| character | profile | uuid-A | `{"name":"Tom","lv":10}` |
| character | stats | uuid-A | `{"atk":120,"def":80}` |
| bag | items | uuid-A | `{"slots":[...]}` |
| quest | main | uuid-A | `{"chapter":3}` |

## API

### Store

| 方法 | 说明 |
|------|------|
| `New(ctx, logger, config)` | 创建 Store 实例，自动连接数据库并初始化索引 |
| `Write(ctx, ops)` | 批量写入，返回 `[]*ObjectAck` |
| `Read(ctx, objectIDs)` | 批量读取，返回 `[]*Object` |
| `List(ctx, ownerID, collection, limit, cursor)` | 游标分页列表查询 |
| `Delete(ctx, ops)` | 批量删除 |
| `ExecuteInTx(ctx, fn)` | 在事务中执行自定义操作 |
| `Close(ctx)` | 关闭数据库连接 |

### 错误

| 错误 | 触发条件 |
|------|----------|
| `ErrRejectedVersion` | 写入/删除时 version 不匹配 |
| `ErrNotFound` | 带 version 删除时目标不存在 |

### 配置

```go
config := storage.NewDefaultDatabaseConfig()
```

| 字段 | 默认值 | 说明 |
|------|--------|------|
| URI | `mongodb://localhost:27017` | MongoDB 连接地址 |
| Database | `storage` | 数据库名 |
| MaxPoolSize | `100` | 最大连接池大小 |
| MinPoolSize | `10` | 最小连接池大小 |
| MaxConnIdleTimeMs | `600000` | 空闲连接超时 (毫秒) |

## 索引设计

初始化时自动创建以下索引：

| 索引 | 字段 | 用途 |
|------|------|------|
| 主键索引 (唯一) | `collection + key + player_id` | 精确读写 |
| 列表索引 | `collection + player_id + key` | 分页查询 |
| 玩家索引 | `player_id` | 按玩家查询 |

索引数量**固定**，不随数据类型增长。所有查询以 `collection` 开头，遵循 ESR 原则。

## 扩容方案

| 规模 | 方案 |
|---|---|
| 10 万玩家 (~100 万文档) | 单机 + 索引 |
| 100 万玩家 (~1000 万文档) | 单机 + 副本集 (高可用) |
| 1000 万玩家 (~1 亿文档) | 2~3 个分片 |
| 1 亿玩家 (~10 亿文档) | 10+ 个分片 |

## 不适合的场景

以下功能建议使用独立 collection：

- **排行榜** — 需要跨用户排序查询
- **好友关系** — 双向关系，需要 JOIN 式查询
- **公会/群组** — 多对多关系，成员列表查询
- **交易市场** — 需要按价格、类型等字段做范围查询

## 测试

需要本地运行的 MongoDB 实例：

```bash
# 使用默认地址 mongodb://localhost:27017
go test ./...

# 指定 MongoDB 地址
TEST_MONGODB_URI="mongodb://user:pass@host:port" go test ./...
```

## 依赖

- [MongoDB Go Driver v2](https://pkg.go.dev/go.mongodb.org/mongo-driver/v2) — 数据库驱动
- [Zap](https://pkg.go.dev/go.uber.org/zap) — 结构化日志

## 其他

感谢 [Nakama](https://github.com/heroiclabs/nakama) 提供的设计思路 ٩(｡・ω・｡)و 


## License

MIT
