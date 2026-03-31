package storage

import (
	"crypto/md5"
	"encoding/hex"
	"time"
)

// Object 存储对象的完整表示, 对应数据库中的一条文档
//
// 主键由 Collection + Key + PlayerID 三者组合构成唯一标识
type Object struct {
	// 集合名称, 用于逻辑分组
	Collection string `bson:"collection" json:"collection"`
	// 对象在集合内的唯一键
	Key string `bson:"key" json:"key"`
	// 对象玩家 ID
	PlayerID string `bson:"player_id" json:"player_id"`
	// 对象的值, 存储为 JSON 字符串
	Value string `bson:"value" json:"value"`
	// 值的 MD5 哈希, 用于乐观锁并发控制
	Version string `bson:"version" json:"version"`
	// 创建时间
	CreateTime time.Time `bson:"create_time" json:"create_time"`
	// 最后更新时间
	UpdateTime time.Time `bson:"update_time" json:"update_time"`
}

// WriteObject 写入请求的参数
type WriteObject struct {
	// 目标集合名称
	Collection string `json:"collection"`
	// 对象键
	Key string `json:"key"`
	// 对象值, JSON 字符串
	Value string `json:"value"`
	// 版本号, 用于乐观锁: 空字符串=不检查版本, 非空=必须匹配才更新
	Version string `json:"version"`
}

// ReadObjectID 读取请求的定位标识
type ReadObjectID struct {
	// 集合名称
	Collection string `json:"collection"`
	// 对象键
	Key string `json:"key"`
	// 玩家 ID
	PlayerID string `json:"player_id"`
}

// DeleteObjectID 删除请求的定位标识
type DeleteObjectID struct {
	// 集合名称
	Collection string `json:"collection"`
	// 对象键
	Key string `json:"key"`
	// 版本号, 非空时必须匹配才允许删除
	Version string `json:"version"`
}

// OpWrite 内部写操作, 绑定所有者与写入请求
type OpWrite struct {
	// 对象玩家 ID
	OwnerID string
	// 写入请求参数
	Object *WriteObject
}

// ExpectedVersion 计算写入后的预期版本号 (值的 MD5 哈希)
//
// 返回值:
//   - string: 16 字节 MD5 哈希的十六进制字符串
func (op *OpWrite) ExpectedVersion() string {
	hash := md5.Sum([]byte(op.Object.Value))
	return hex.EncodeToString(hash[:])
}

// OpWrites 批量写操作, 实现 sort.Interface 保证操作顺序一致以防止死锁
type OpWrites []*OpWrite

func (s OpWrites) Len() int      { return len(s) }
func (s OpWrites) Swap(i, j int) { s[i], s[j] = s[j], s[i] }

// Less 排序对象
// 按字符串的字母序排, 避免死锁
// 不同事务可以并行, 排序发生在事务开始之前, 两个事务同时跑, 它们的锁对象顺序一致, 所以不会死锁
func (s OpWrites) Less(i, j int) bool {
	s1, s2 := s[i], s[j]
	if s1.Object.Collection != s2.Object.Collection {
		return s1.Object.Collection < s2.Object.Collection
	}
	if s1.Object.Key != s2.Object.Key {
		return s1.Object.Key < s2.Object.Key
	}
	return s1.OwnerID < s2.OwnerID
}

// OpDelete 内部删除操作, 绑定所有者与删除标识
type OpDelete struct {
	// 对象玩家 ID
	OwnerID string
	// 删除目标的定位标识
	ObjectID *DeleteObjectID
}

// OpDeletes 批量删除操作, 实现 sort.Interface 保证操作顺序一致以防止死锁
type OpDeletes []*OpDelete

func (s OpDeletes) Len() int      { return len(s) }
func (s OpDeletes) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s OpDeletes) Less(i, j int) bool {
	s1, s2 := s[i], s[j]
	if s1.ObjectID.Collection != s2.ObjectID.Collection {
		return s1.ObjectID.Collection < s2.ObjectID.Collection
	}
	if s1.ObjectID.Key != s2.ObjectID.Key {
		return s1.ObjectID.Key < s2.ObjectID.Key
	}
	return s1.OwnerID < s2.OwnerID
}
