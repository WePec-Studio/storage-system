# Gavin 的游戏通用存储方案

基于 MongoDB (ง ˙o˙)ว

## 核心设计思路

### a) 乐观并发控制

写入时对 `value` 做 MD5 生成 `version`。客户端更新时携带 version，服务端对比不一致则拒绝写入，避免并发覆盖。无锁设计，高并发友好。

### b) 权限模型内嵌到数据行

`read`/`write` 字段直接存储在文档上，查询时直接过滤，不需要额外的 ACL 表：

```javascript
// 查询时自动过滤权限
{ $or: [
    { read: 2 },                                    // 公开数据
    { read: 1, user_id: callerId }                  // 仅自己可读
]}
```

### c) 事务重试策略
(´・ω・`) 还在思考中

### d) 可选的内存全文搜索索引

在数据库之上叠加一层可选的内存索引，支持全文搜索、字段过滤、排序。热数据走内存，冷数据走 DB，性能和灵活性兼得。

### e) Schema-Free 自然兼容

value 为 BSON，字段变更时旧数据自然兼容，新增字段读出为零值，无需停服迁移。

### f) 批量操作 + 排序防死锁

写入/删除支持批量操作，操作前对 key 排序以固定加锁顺序，避免死锁。

## 数据模型

同一张 collection 存储所有类型数据：

| collection | key | user_id | value |
|---|---|---|---|
| player | profile | uuid-A | `{"name":"Tom","lv":10}` |
| role | warrior | uuid-A | `{"class":1,"hp":999}` |
| bag | items | uuid-A | `{"slots":[...]}` |
| bag | items | uuid-B | `{"slots":[...]}` |

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

## 相比传统多表方案的优势

TODO

## 扩容方案

| 规模 | 方案 |
|---|---|
| 10 万玩家（~100 万文档） | 单机 + 索引 |
| 100 万玩家（~1000 万文档） | 单机 + 副本集（高可用） |
| 1000 万玩家（~1 亿文档） | 2~3 个分片 |
| 1 亿玩家（~10 亿文档） | 10+ 个分片 |

## 不适合的场景

以下功能建议使用独立 collection：

- **排行榜** —— 需要跨用户排序查询
- **好友关系** —— 双向关系、需要 JOIN 式查询
- **公会/群组** —— 多对多关系、成员列表查询
- **交易市场** —— 需要按价格、类型等字段做范围查询
