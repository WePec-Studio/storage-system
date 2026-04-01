package storage

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"sort"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.uber.org/zap"
)

var (
	// ErrRejectedVersion 版本冲突, 乐观锁检查未通过
	ErrRejectedVersion = errors.New("storage write rejected: version check failed")
	// ErrNotFound 目标对象不存在
	ErrNotFound = errors.New("storage object not found")
)

// ObjectAck 写入成功后的确认信息
type ObjectAck struct {
	// 集合名称
	Collection string
	// 对象键
	Key string
	// 写入后的版本号
	Version string
	// 玩家 ID
	PlayerID string
	// 创建时间
	CreateTime time.Time
	// 最后更新时间
	UpdateTime time.Time
}

// WriteObjects 批量写入存储对象
//
// 支持两种写入模式, 由 WriteObject.Version 控制:
//   - 空字符串: 存在则更新, 不存在则插入 (最后写入者胜出)
//   - 非空值: 乐观锁, 版本匹配时才更新, 不匹配返回 ErrRejectedVersion
//
// 参数:
//   - ctx: 上下文
//   - logger: 日志记录器
//   - coll: MongoDB Collection 实例
//   - ops: 批量写操作
//
// 返回值:
//   - []*ObjectAck: 写入确认列表, 顺序与输入一致
//   - error: ErrRejectedVersion 或其他错误
func WriteObjects(ctx context.Context, logger *zap.Logger, coll *mongo.Collection, ops OpWrites) ([]*ObjectAck, error) {
	if len(ops) == 0 {
		return make([]*ObjectAck, 0), nil
	}

	// 排序防死锁, 在副本上排序以保留原始顺序
	sortedOps := make(OpWrites, len(ops))
	indexMap := make(map[*OpWrite]int, len(ops))
	for i, op := range ops {
		sortedOps[i] = op
		indexMap[op] = i
	}
	sort.Sort(sortedOps)

	acks := make([]*ObjectAck, len(ops))
	now := time.Now()

	for _, op := range sortedOps {
		ack, err := writeOneObject(ctx, logger, coll, op, now)
		if err != nil {
			return nil, err
		}
		acks[indexMap[op]] = ack
	}

	return acks, nil
}

// writeOneObject 执行单个对象的写入操作
//
// 参数:
//   - ctx: 上下文
//   - logger: 日志记录器
//   - coll: MongoDB Collection 实例
//   - op: 写操作
//   - now: 当前时间, 保证同一批次写入的时间一致
//
// 返回值:
//   - *ObjectAck: 写入确认
//   - error: 写入失败时的错误
func writeOneObject(ctx context.Context, logger *zap.Logger, coll *mongo.Collection, op *OpWrite, now time.Time) (*ObjectAck, error) {
	object := op.Object
	ownerID := op.OwnerID

	newVersion := computeVersion(object.Value)
	// 主键过滤条件
	filter := bson.M{
		"collection": object.Collection,
		"key":        object.Key,
		"player_id":  ownerID,
	}

	if object.Version == "" {
		// 不检查版本: 存在则更新, 不存在则插入
		update := bson.M{
			"$set": bson.M{
				"value":       object.Value,
				"version":     newVersion,
				"update_time": now,
			},
			"$setOnInsert": bson.M{
				"collection":  object.Collection,
				"key":         object.Key,
				"player_id":   ownerID,
				"create_time": now,
			},
		}

		opts := options.FindOneAndUpdate().
			SetUpsert(true).
			SetReturnDocument(options.After)

		var result Object
		// WiredTiger 引擎自动对文档加意向写锁
		err := coll.FindOneAndUpdate(ctx, filter, update, opts).Decode(&result)
		if err != nil {
			logger.Error("写入存储对象失败", zap.Error(err))
			return nil, err
		}

		return &ObjectAck{
			Collection: result.Collection,
			Key:        result.Key,
			Version:    result.Version,
			PlayerID:   result.PlayerID,
			CreateTime: result.CreateTime,
			UpdateTime: result.UpdateTime,
		}, nil
	}

	// 乐观锁: 版本匹配才更新
	filter["version"] = object.Version

	update := bson.M{
		"$set": bson.M{
			"value":       object.Value,
			"version":     newVersion,
			"update_time": now,
		},
	}

	opts := options.FindOneAndUpdate().
		SetReturnDocument(options.After)

	var result Object
	err := coll.FindOneAndUpdate(ctx, filter, update, opts).Decode(&result)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, ErrRejectedVersion
		}
		logger.Error("更新存储对象失败", zap.Error(err))
		return nil, err
	}

	return &ObjectAck{
		Collection: result.Collection,
		Key:        result.Key,
		Version:    result.Version,
		PlayerID:   result.PlayerID,
		CreateTime: result.CreateTime,
		UpdateTime: result.UpdateTime,
	}, nil
}

// computeVersion 计算值的 MD5 哈希作为版本号
//
// 参数:
//   - value: 对象值
//
// 返回值:
//   - string: 16 字节 MD5 哈希的十六进制字符串
func computeVersion(value string) string {
	hash := md5.Sum([]byte(value))
	return hex.EncodeToString(hash[:])
}

// ReadObjects 按 ID 批量读取存储对象
//
// 参数:
//   - ctx: 上下文
//   - logger: 日志记录器
//   - coll: MongoDB Collection 实例
//   - objectIDs: 要读取的对象定位标识列表
//
// 返回值:
//   - []*Object: 读取到的对象列表, 不存在的对象会被跳过
//   - error: 数据库错误
func ReadObjects(ctx context.Context, logger *zap.Logger, coll *mongo.Collection, objectIDs []*ReadObjectID) ([]*Object, error) {
	if len(objectIDs) == 0 {
		return make([]*Object, 0), nil
	}

	// 构建 $or 查询, 每个 ID 一个条件
	orConditions := make(bson.A, 0, len(objectIDs))
	for _, id := range objectIDs {
		orConditions = append(orConditions, bson.M{
			"collection": id.Collection,
			"key":        id.Key,
			"player_id":  id.PlayerID,
		})
	}

	filter := bson.M{"$or": orConditions}

	cursor, err := coll.Find(ctx, filter)
	if err != nil {
		logger.Error("读取存储对象失败", zap.Error(err))
		return nil, err
	}
	defer cursor.Close(ctx)

	var objects []*Object
	if err = cursor.All(ctx, &objects); err != nil {
		logger.Error("解析存储对象失败", zap.Error(err))
		return nil, err
	}

	if objects == nil {
		objects = make([]*Object, 0)
	}

	return objects, nil
}

// DeleteObjects 批量删除存储对象
//
// 参数:
//   - ctx: 上下文
//   - logger: 日志记录器
//   - coll: MongoDB Collection 实例
//   - ops: 批量删除操作
//
// 返回值:
//   - error: ErrNotFound 或数据库错误
func DeleteObjects(ctx context.Context, logger *zap.Logger, coll *mongo.Collection, ops OpDeletes) error {
	if len(ops) == 0 {
		return nil
	}

	// 排序防死锁
	sort.Sort(ops)

	for _, op := range ops {
		filter := bson.M{
			"collection": op.ObjectID.Collection,
			"key":        op.ObjectID.Key,
			"player_id":  op.OwnerID,
		}

		// 带版本号的条件删除
		if op.ObjectID.Version != "" {
			filter["version"] = op.ObjectID.Version
		}

		result, err := coll.DeleteOne(ctx, filter)
		if err != nil {
			logger.Error("删除存储对象失败", zap.Error(err), zap.Any("object_id", op.ObjectID))
			return err
		}

		// 不带版本号: 删 0 行也算成功, 幂等
		if op.ObjectID.Version == "" {
			continue
		}

		// version != "" 时, 删 0 行报错, 因为调用方明确说了"我要删 version=abc123"，删不到说明状态不符预期，应该让调用方知道
		if result.DeletedCount == 0 {
			return ErrNotFound
		}
	}

	return nil
}
