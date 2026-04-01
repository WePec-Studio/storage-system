package storage

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.uber.org/zap"
)

// Collection 名称常量
const collectionStorage = "storage"

// InitCollection 初始化 storage Collection 及其索引
//
// 创建与查询模式匹配的复合索引
// 索引设计遵循 ESR 原则 (Equality, Sort, Range)
//
// 参数:
//   - ctx: 上下文
//   - logger: 日志记录器
//   - db: MongoDB 数据库实例
//
// 返回值:
//   - *mongo.Collection: 初始化完成的 Collection 实例
//   - error: 索引创建失败时的错误
func InitCollection(ctx context.Context, logger *zap.Logger, db *mongo.Database) (*mongo.Collection, error) {
	coll := db.Collection(collectionStorage)

	initCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	indexes := []mongo.IndexModel{
		// 主键索引: collection + key + player_id 唯一约束 (精准定位一个对象)
		{
			Keys:    bson.D{{Key: "collection", Value: 1}, {Key: "key", Value: 1}, {Key: "player_id", Value: 1}},
			Options: options.Index().SetUnique(true).SetName("idx_collection_key_playerid_unique"),
		},
		// 按 collection + player_id + key 查询 (列表某玩家在某集合下的对象)
		{
			Keys:    bson.D{{Key: "collection", Value: 1}, {Key: "player_id", Value: 1}, {Key: "key", Value: 1}},
			Options: options.Index().SetName("idx_collection_playerid_key"),
		},
		// 按玩家查询 (玩家关联查询场景)
		{
			Keys:    bson.D{{Key: "player_id", Value: 1}},
			Options: options.Index().SetName("idx_playerid"),
		},
	}

	_, err := coll.Indexes().CreateMany(initCtx, indexes)
	if err != nil {
		return nil, fmt.Errorf("create storage indexes: %w", err)
	}

	logger.Info("storage Collection 初始化完成", zap.Int("index_count", len(indexes)))

	return coll, nil
}
