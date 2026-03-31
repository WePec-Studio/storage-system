package storage

import (
	"context"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.uber.org/zap"
)

// MongoConnect 建立 MongoDB 连接并验证连通性
//
// 解析配置中的连接地址, 设置连接池参数, 执行健康检查,
// 连接失败时直接终止进程
//
// 参数:
//   - ctx: 上下文, 用于控制连接超时
//   - logger: 日志记录器
//   - config: 数据库连接配置
//
// 返回值:
//   - *mongo.Client: MongoDB 客户端实例
//   - *mongo.Database: 指定的数据库实例
func MongoConnect(ctx context.Context, logger *zap.Logger, config DatabaseConfig) (*mongo.Client, *mongo.Database) {
	// 构建连接 URI
	uri := config.URI
	if uri == "" {
		logger.Fatal("数据库连接 URI 不能为空")
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
		logger.Fatal("MongoDB 连接失败", zap.Error(err))
	}

	// 健康检查, 限制 15 秒超时
	pingCtx, pingCancel := context.WithTimeout(ctx, 15*time.Second)
	defer pingCancel()

	if err = client.Ping(pingCtx, nil); err != nil {
		logger.Fatal("MongoDB Ping 失败", zap.Error(err))
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

	return client, db
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
