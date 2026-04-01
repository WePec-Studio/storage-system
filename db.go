package storage

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.uber.org/zap"
)

// 事务重试相关常量
const (
	// 事务最大重试次数
	maxRetries = 5
	// MongoDB 瞬时事务错误标签, 表示整个事务可以重试
	labelTransientTransaction = "TransientTransactionError"
	// MongoDB 提交结果未知标签, 表示仅 commit 可以重试
	labelUnknownCommitResult = "UnknownTransactionCommitResult"
)

// labeledError 用于判断错误是否携带 MongoDB 错误标签
type labeledError interface {
	HasErrorLabel(string) bool
}

// hasErrorLabel 判断 err 是否携带指定的 MongoDB 错误标签
func hasErrorLabel(err error, label string) bool {
	var le labeledError
	if errors.As(err, &le) {
		return le.HasErrorLabel(label)
	}
	return false
}

// ExecuteInTx 在 MongoDB 事务中执行 fn, 遇到瞬时错误自动重试
//
// 失败时 abort 当前事务, 重新开启新事务执行 fn, 最多重试 maxRetries 次.
// 对于 commit 阶段的 UnknownTransactionCommitResult 错误,
// 仅重试 commit 而不重跑 fn
//
// 参数:
//   - ctx: 上下文
//   - client: MongoDB 客户端, 用于创建 Session
//   - fn: 事务内执行的业务逻辑, 参数为绑定了 Session 的 context
//
// 返回值:
//   - error: 不可重试的错误或超过重试次数后的最后一次错误
func ExecuteInTx(ctx context.Context, client *mongo.Client, fn func(ctx context.Context) error) error {
	session, err := client.StartSession()
	if err != nil {
		return err
	}
	defer session.EndSession(ctx)

	for i := 0; i < maxRetries; i++ {
		err = executeOnce(ctx, session, fn)
		if err == nil {
			return nil
		}
		// 操作阶段或 commit 阶段的瞬时错误, 整个事务重试
		if hasErrorLabel(err, labelTransientTransaction) {
			continue
		}
		return err
	}

	return err
}

// executeOnce 执行一次完整的事务流程: 开启事务 → 执行 fn → 提交
//
// commit 阶段遇到 UnknownTransactionCommitResult 时仅重试 commit,
// 不重跑 fn
//
// 参数:
//   - ctx: 上下文
//   - session: MongoDB Session
//   - fn: 事务内执行的业务逻辑
//
// 返回值:
//   - error: 事务执行或提交过程中的错误
func executeOnce(ctx context.Context, session *mongo.Session, fn func(ctx context.Context) error) error {
	if err := session.StartTransaction(); err != nil {
		return err
	}

	// 执行 fn 中的业务逻辑，数据已经写到 MongoDB 的事务缓冲区了
	err := mongo.WithSession(ctx, session, fn)
	if err != nil {
		_ = session.AbortTransaction(ctx)
		return err
	}

	for i := 0; i < maxRetries; i++ {
		// 把操作的结果真正提交到数据库，如果失败，返回 UnknownTransactionCommitResult 仅重试 commit
		err = session.CommitTransaction(ctx)
		if err == nil {
			return nil
		}
		if !hasErrorLabel(err, labelUnknownCommitResult) {
			break
		}
	}

	return err
}

// MongoConnect 建立 MongoDB 连接并验证连通性
//
// 解析配置中的连接地址, 设置连接池参数, 执行健康检查
//
// 参数:
//   - ctx: 上下文, 用于控制连接超时
//   - logger: 日志记录器
//   - config: 数据库连接配置
//
// 返回值:
//   - *mongo.Client: MongoDB 客户端实例
//   - *mongo.Database: 指定的数据库实例
//   - error: 连接或健康检查失败时的错误
func MongoConnect(ctx context.Context, logger *zap.Logger, config DatabaseConfig) (*mongo.Client, *mongo.Database, error) {
	// 构建连接 URI
	uri := config.URI
	if uri == "" {
		return nil, nil, errors.New("数据库连接 URI 不能为空")
	}

	// 配置客户端选项
	opts := options.Client().
		ApplyURI(uri). // MongoDB 驱动内部解析 URI
		SetMaxPoolSize(config.MaxPoolSize).
		SetMinPoolSize(config.MinPoolSize).
		SetMaxConnIdleTime(time.Duration(config.MaxConnIdleTimeMs) * time.Millisecond)

	// 建立连接
	client, err := mongo.Connect(opts)
	if err != nil {
		return nil, nil, fmt.Errorf("mongodb connect: %w", err)
	}

	// 健康检查, 限制 15 秒超时
	pingCtx, pingCancel := context.WithTimeout(ctx, 15*time.Second)
	defer pingCancel()

	if err = client.Ping(pingCtx, nil); err != nil {
		return nil, nil, fmt.Errorf("mongodb ping: %w", err)
	}

	// 获取并记录数据库版本信息
	var buildInfo bson.M
	err = client.Database("admin").RunCommand(pingCtx, bson.D{{Key: "buildInfo", Value: 1}}).Decode(&buildInfo)
	if err != nil {
		logger.Warn("获取 MongoDB 版本信息失败", zap.Error(err))
	} else {
		logger.Info("MongoDB 连接成功",
			zap.Any("version", buildInfo["version"]),
		)
	}

	// 获取目标数据库
	dbName := config.Database
	if dbName == "" {
		dbName = "storage"
	}
	db := client.Database(dbName)

	logger.Info("数据库已就绪", zap.String("database", dbName))

	return client, db, nil
}

// DatabaseConfig MongoDB 连接配置
type DatabaseConfig struct {
	// 连接 URI, 格式: mongodb://user:pass@host:port
	URI string `yaml:"uri" json:"uri"`
	// 数据库名称, 默认 "storage"
	Database string `yaml:"database" json:"database"`
	// 连接池最大连接数
	// 防止把 MongoDB 内存吃光或触发文件描述符上限
	MaxPoolSize uint64 `yaml:"max_pool_size" json:"max_pool_size"`
	// 连接池最小连接数
	// 保证冷启动不卡
	MinPoolSize uint64 `yaml:"min_pool_size" json:"min_pool_size"`
	// 空闲连接最大存活时间, 单位毫秒
	// 回收白占着资源的连接, 云环境的空闲 TCP 超时一般 5 ~ 10 分钟, 如果被悄悄断开, ops 就会报错
	MaxConnIdleTimeMs int `yaml:"max_conn_idle_time_ms" json:"max_conn_idle_time_ms"`
}

// NewDefaultDatabaseConfig 创建带默认值的数据库配置
//
// 返回值:
//   - DatabaseConfig: 默认连接池 100, 最小 10, 空闲超时 600000ms (10 分钟)
func NewDefaultDatabaseConfig() DatabaseConfig {
	return DatabaseConfig{
		URI:               "mongodb://localhost:27017",
		Database:          "storage",
		MaxPoolSize:       100,
		MinPoolSize:       10,
		MaxConnIdleTimeMs: 600000,
	}
}
