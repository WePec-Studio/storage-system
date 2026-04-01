package storage

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.uber.org/zap"
)

// Store 存储引擎, 封装 MongoDB 连接和 Collection, 提供统一的 CRUD 入口
type Store struct {
	// 日志记录器
	logger *zap.Logger
	// MongoDB 客户端
	client *mongo.Client
	// MongoDB 数据库
	db *mongo.Database
	// storage Collection
	coll *mongo.Collection
}

// New 创建存储服务实例
//
// 初始化 MongoDB 连接, 创建 Collection 及索引
//
// 参数:
//   - ctx: 上下文
//   - logger: 日志记录器
//   - config: 数据库连接配置
//
// 返回值:
//   - *Store: 存储服务实例
//   - error: 连接或初始化失败时的错误
func New(ctx context.Context, logger *zap.Logger, config DatabaseConfig) (*Store, error) {
	client, db, err := MongoConnect(ctx, logger, config)
	if err != nil {
		return nil, fmt.Errorf("storage new: %w", err)
	}

	coll, err := InitCollection(ctx, logger, db)
	if err != nil {
		return nil, fmt.Errorf("storage new: %w", err)
	}

	return &Store{
		logger: logger,
		client: client,
		db:     db,
		coll:   coll,
	}, nil
}

// Write 批量写入存储对象
//
// 参数:
//   - ctx: 上下文
//   - ops: 批量写操作
//
// 返回值:
//   - []*ObjectAck: 写入确认列表, 顺序与输入一致
//   - error: ErrRejectedVersion 或其他错误
func (s *Store) Write(ctx context.Context, ops OpWrites) ([]*ObjectAck, error) {
	return WriteObjects(ctx, s.logger, s.coll, ops)
}

// Read 按 ID 批量读取存储对象
//
// 参数:
//   - ctx: 上下文
//   - objectIDs: 要读取的对象定位标识列表
//
// 返回值:
//   - []*Object: 读取到的对象列表, 不存在的对象会被跳过
//   - error: 数据库错误
func (s *Store) Read(ctx context.Context, objectIDs []*ReadObjectID) ([]*Object, error) {
	return ReadObjects(ctx, s.logger, s.coll, objectIDs)
}

// List 按条件列表查询存储对象, 支持游标分页
//
// 参数:
//   - ctx: 上下文
//   - ownerID: 目标玩家 ID, 空字符串表示不限玩家
//   - collection: 集合名称
//   - limit: 每页数量
//   - cursor: 分页游标, 空字符串表示从头开始
//
// 返回值:
//   - *ObjectList: 查询结果, 包含对象列表和下一页游标
//   - error: 参数错误或数据库错误
func (s *Store) List(ctx context.Context, ownerID string, collection string, limit int, cursor string) (*ObjectList, error) {
	return ListObjects(ctx, s.logger, s.coll, ownerID, collection, limit, cursor)
}

// Delete 批量删除存储对象
//
// 参数:
//   - ctx: 上下文
//   - ops: 批量删除操作
//
// 返回值:
//   - error: ErrNotFound 或数据库错误
func (s *Store) Delete(ctx context.Context, ops OpDeletes) error {
	return DeleteObjects(ctx, s.logger, s.coll, ops)
}

// ExecuteInTx 在事务中执行 fn, 遇到瞬时错误自动重试
//
// 参数:
//   - ctx: 上下文
//   - fn: 事务内执行的业务逻辑, 参数为绑定了 Session 的 context
//
// 返回值:
//   - error: 不可重试的错误或超过重试次数后的最后一次错误
func (s *Store) ExecuteInTx(ctx context.Context, fn func(ctx context.Context) error) error {
	return ExecuteInTx(ctx, s.client, fn)
}

// Close 关闭 MongoDB 连接
//
// 参数:
//   - ctx: 上下文
//
// 返回值:
//   - error: 关闭连接时的错误
func (s *Store) Close(ctx context.Context) error {
	return s.client.Disconnect(ctx)
}
