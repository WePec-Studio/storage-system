package storage

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/gob"
	"errors"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.uber.org/zap"
)

// listCursor 分页游标, 记录上一页最后一条记录的位置
type listCursor struct {
	// 对象键
	Key string
	// 玩家 ID
	PlayerID string
}

// ObjectList 列表查询结果
type ObjectList struct {
	// 本页对象列表
	Objects []*Object
	// 下一页游标, 空字符串表示已无更多数据
	Cursor string
}

// ListObjects 按条件列表查询存储对象, 支持游标分页
//
// 参数:
//   - ctx: 上下文
//   - logger: 日志记录器
//   - coll: MongoDB Collection 实例
//   - ownerID: 目标玩家 ID, 空字符串表示不限玩家
//   - collection: 集合名称
//   - limit: 每页数量
//   - cursor: 分页游标, 空字符串表示从头开始
//
// 返回值:
//   - *ObjectList: 查询结果, 包含对象列表和下一页游标
//   - error: 参数错误或数据库错误
func ListObjects(ctx context.Context, logger *zap.Logger, coll *mongo.Collection, ownerID string, collection string, limit int, cursor string) (*ObjectList, error) {
	if limit <= 0 {
		return &ObjectList{Objects: make([]*Object, 0)}, nil
	}

	// 解析游标
	var sc *listCursor
	if cursor != "" {
		sc = &listCursor{}
		cb, err := base64.RawURLEncoding.DecodeString(cursor)
		if err != nil {
			logger.Warn("游标 base64 解码失败", zap.String("cursor", cursor))
			return nil, errors.New("malformed cursor")
		}
		if err = gob.NewDecoder(bytes.NewReader(cb)).Decode(sc); err != nil {
			logger.Warn("游标 gob 解码失败", zap.String("cursor", cursor))
			return nil, errors.New("malformed cursor")
		}
	}

	// 构建查询条件
	filter := bson.M{"collection": collection}

	if ownerID != "" {
		filter["player_id"] = ownerID
	}

	// 游标分页: 基于排序键做范围查询
	if sc != nil {
		cursorFilter := bson.M{
			"$or": bson.A{
				bson.M{"key": bson.M{"$gt": sc.Key}},
				bson.M{"key": sc.Key, "player_id": bson.M{"$gt": sc.PlayerID}},
			},
		}
		filter = bson.M{"$and": bson.A{filter, cursorFilter}}
	}

	// 多取一条用于判断是否有下一页
	findOpts := options.Find().
		SetSort(bson.D{{Key: "key", Value: 1}, {Key: "player_id", Value: 1}}).
		SetLimit(int64(limit + 1))

	mongoCursor, err := coll.Find(ctx, filter, findOpts)
	if err != nil {
		logger.Error("列表查询存储对象失败", zap.Error(err))
		return nil, err
	}
	defer mongoCursor.Close(ctx)

	var objects []*Object
	if err = mongoCursor.All(ctx, &objects); err != nil {
		logger.Error("解析列表结果失败", zap.Error(err))
		return nil, err
	}

	if objects == nil {
		objects = make([]*Object, 0)
	}

	// 判断是否有下一页, 构建游标
	var nextCursor string
	if len(objects) > limit {
		// 截掉多取的那一条
		objects = objects[:limit]
		last := objects[limit-1]
		nextCursor = encodeCursor(&listCursor{
			Key:      last.Key,
			PlayerID: last.PlayerID,
		})
	}

	return &ObjectList{
		Objects: objects,
		Cursor:  nextCursor,
	}, nil
}

// encodeCursor 将游标结构体编码为 base64 字符串
//
// 参数:
//   - sc: 游标结构体
//
// 返回值:
//   - string: base64 编码的游标字符串
func encodeCursor(sc *listCursor) string {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(sc); err != nil {
		return ""
	}
	return base64.RawURLEncoding.EncodeToString(buf.Bytes())
}
