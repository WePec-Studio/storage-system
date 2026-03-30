# Storage System

基于 MongoDB 的游戏通用存储方案。

## 核心设计思路

### 乐观并发控制（OCC）

写入时对 `value` 做 MD5 生成 `version`。客户端更新时携带 version，服务端对比不一致则拒绝写入，避免并发覆盖。无锁设计，高并发友好。

### 权限模型内嵌到数据行

`read`/`write` 字段直接存储在文档上，查询时直接过滤，不需要额外的 ACL 表：

```javascript
// 查询时自动过滤权限
{ $or: [
    { read: 2 },                                    // 公开数据
    { read: 1, user_id: callerId }                   // 仅自己可读
]}
```

### 批量操作 + 排序防死锁

写入/删除支持批量操作，操作前对 key 排序以固定加锁顺序，避免死锁。

### 可选的内存搜索索引

在数据库之上叠加一层可选的内存索引，支持全文搜索、字段过滤、排序。热数据走内存，冷数据走 DB，性能和灵活性兼得。

### Schema-Free 自然兼容

value 为 JSON/BSON，字段变更时旧数据自然兼容，新增字段读出为零值，无需停服迁移。

## 数据模型

```javascript
// MongoDB collection: storage

{
  collection: "bag",                    // 逻辑分组（背包、角色、邮件...）
  key:        "items",                  // 对象名称
  user_id:    "1978664932597575680",    // 所有者
  value: {                              // 实际业务数据（任意结构）
    "5c763714-...": {
      "id": "5c763714-...",
      "cfgid": 13,
      "count": 5,
      "rarity": 5
      // ...
    }
  },
  version:     "a1b2c3d4...",           // value 的 MD5，用于乐观并发控制（OCC）
  read:        1,                       // 读权限：0=不可读, 1=仅自己, 2=公开
  write:       1,                       // 写权限：0=不可写, 1=仅自己/服务端
  create_time: ISODate(),
  update_time: ISODate()
}
```

同一张 collection 存储所有类型数据：

| collection | key | user_id | value |
|---|---|---|---|
| player | profile | uuid-A | `{"name":"Tom","lv":10}` |
| role | warrior | uuid-A | `{"class":1,"hp":999}` |
| bag | items | uuid-A | `{"slots":[...]}` |
| bag | items | uuid-B | `{"slots":[...]}` |
| mail | inbox | uuid-A | `{"msgs":[...]}` |
| achieve | list | uuid-A | `{"ids":[1,2,3]}` |

## 索引设计

TODO

索引数量**固定**，不随数据类型增长。所有查询以 `collection` 开头，B-Tree 中等效按类型分区，查询时间始终 O(log N)。

## 核心 CRUD 操作

```go
// 一套通用方法处理所有数据类型
func Read(collection, key, userID string) (bson.M, error)
func Write(collection, key, userID string, value interface{}) error
func Delete(collection, key, userID string) error
func List(collection, userID string, cursor string) ([]bson.M, error)
```

### 写入（Upsert + OCC）

```go
filter := bson.M{
    "collection": "bag",
    "key":        "items",
    "user_id":    userID,
}
update := bson.M{
    "$set": bson.M{
        "value":       bagData,
        "version":     md5(bagData),
        "update_time": time.Now(),
    },
    "$setOnInsert": bson.M{
        "create_time": time.Now(),
        "read":        1,
        "write":       1,
    },
}
collection.UpdateOne(ctx, filter, update, options.Update().SetUpsert(true))
```

## 权限控制

| read 值 | 含义 | 场景 |
|---|---|---|
| 1 | 仅自己可读 | 背包、私有存档 |
| 2 | 公开可读 | 玩家展示的角色、排行信息 |

```go
if doc.Read == 1 && callerID != doc.UserID {
    return ErrForbidden
}
```

## 写入流程

```
Client 请求写入
  → API 层：校验 JSON 格式 + 权限检查（write 字段）
    → Storage 层：计算 value 的 MD5 生成 version
      → DB 层：Upsert 文档
        → 成功：返回确认（含服务端计算的 version）
        → 版本冲突：返回错误，由客户端决定重试或放弃
        → 序列化冲突：自动重试（最多 5 次）
```

### 读取流程

```
Client 请求读取
  → API 层：解析 collection + key + user_id
    → Storage 层：构造查询，附加权限过滤条件
      → DB 层：命中复合索引，返回文档
        → 权限校验：read=2 任何人可读，read=1 仅 owner 可读
          → 返回 value + version + 时间戳
```

### 删除流程

```
Client 请求删除
  → API 层：校验权限（write > 0 才允许非服务端删除）
    → Storage 层：可选版本号校验（防止误删已更新的数据）
      → DB 层：删除文档
        → 成功：同步清理内存索引（如有）
```

## 配合 Proto 使用

Proto 定义结构提供强类型保障，序列化后存入 value 字段：

```go
// 定义
message BagData {
    repeated Item slots = 1;
}

// 写入：Proto → JSON → MongoDB
jsonBytes, _ := protojson.Marshal(bagData)
var doc bson.M
bson.UnmarshalExtJSON(jsonBytes, true, &doc)

// 读取：MongoDB → JSON → Proto
var bag BagData
protojson.Unmarshal(rawJSON, &bag)
```

## 相比传统多表方案的优势

TODO

## 扩容方案

| 规模 | 方案 |
|---|---|
| 10 万玩家（~100 万文档） | 单机 + 索引 |
| 100 万玩家（~1000 万文档） | 单机 + 副本集（高可用） |
| 1000 万玩家（~1 亿文档） | 2~3 个分片 |
| 1 亿玩家（~10 亿文档） | 10+ 个分片 |

```javascript
// 分片配置
sh.enableSharding("game")
sh.shardCollection("game.storage", {
    collection: 1,
    user_id: "hashed"
})
```

## 不适合的场景

以下功能建议使用独立 collection：

- **排行榜** —— 需要跨用户排序查询
- **好友关系** —— 双向关系、需要 JOIN 式查询
- **公会/群组** —— 多对多关系、成员列表查询
- **交易市场** —— 需要按价格、类型等字段做范围查询

## 设计原则

- **用索引前缀替代物理分表**：复合索引以 collection 开头，等效于按类型分区，上层代码统一，底层性能等价
- **通用优于专用**：一套 CRUD 处理所有数据类型，减少重复代码
- **无锁优于有锁**：OCC 乐观锁替代悲观锁，适合游戏高并发读多写少场景
- **渐进扩展**：单机 → 副本集 → 分片，架构不变，加机器即可
