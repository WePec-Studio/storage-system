package storage

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.uber.org/zap"
)

// 测试用的 MongoDB 连接地址, 可通过环境变量 TEST_MONGODB_URI 覆盖
// TEST_MONGODB_URI="mongodb://你的用户名:密码@localhost:27017" go test -v -count=1 ./...
func testMongoURI() string {
	if uri := os.Getenv("TEST_MONGODB_URI"); uri != "" {
		return uri
	}
	return "mongodb://localhost:27017"
}

// testSetup 初始化测试环境, 返回 logger, collection 和清理函数
func testSetup(t *testing.T) (*zap.Logger, *mongo.Collection, func()) {
	t.Helper()

	logger, _ := zap.NewDevelopment()
	ctx := context.Background()

	client, err := mongo.Connect(options.Client().ApplyURI(testMongoURI()))
	if err != nil {
		t.Fatalf("连接 MongoDB 失败: %v", err)
	}

	// 每个测试用独立的数据库, 避免并行测试互相干扰
	dbName := fmt.Sprintf("storage_test_%s", t.Name())
	db := client.Database(dbName)
	coll, err := InitCollection(ctx, logger, db)
	if err != nil {
		t.Fatalf("初始化 Collection 失败: %v", err)
	}

	cleanup := func() {
		_ = db.Drop(context.Background())
		_ = client.Disconnect(context.Background())
	}

	return logger, coll, cleanup
}

// --- Write 测试 ---

func TestWriteSingle(t *testing.T) {
	logger, coll, cleanup := testSetup(t)
	defer cleanup()

	ops := OpWrites{
		&OpWrite{
			OwnerID: "player_001",
			Object: &WriteObject{
				Collection: "testcollection",
				Key:        "sword_01",
				Value:      `{"name":"烈焰剑","damage":320}`,
			},
		},
	}

	acks, err := WriteObjects(context.Background(), logger, coll, ops)

	if err != nil {
		t.Fatalf("写入失败: %v", err)
	}
	if len(acks) != 1 {
		t.Fatalf("acks 长度期望 1, 实际 %d", len(acks))
	}
	if acks[0].Collection != "testcollection" {
		t.Errorf("collection 不匹配, 期望 testcollection, 实际 %s", acks[0].Collection)
	}
	if acks[0].Key != "sword_01" {
		t.Errorf("key 不匹配, 期望 sword_01, 实际 %s", acks[0].Key)
	}
	if acks[0].PlayerID != "player_001" {
		t.Errorf("player_id 不匹配, 期望 player_001, 实际 %s", acks[0].PlayerID)
	}

	expectedVersion := computeVersion(`{"name":"烈焰剑","damage":320}`)
	if acks[0].Version != expectedVersion {
		t.Errorf("version 不匹配, 期望 %s, 实际 %s", expectedVersion, acks[0].Version)
	}
}

func TestWriteMultiple(t *testing.T) {
	logger, coll, cleanup := testSetup(t)
	defer cleanup()

	ops := OpWrites{
		&OpWrite{
			OwnerID: "player_001",
			Object: &WriteObject{
				Collection: "testcollection",
				Key:        "item_a",
				Value:      `{"foo":"bar"}`,
			},
		},
		&OpWrite{
			OwnerID: "player_002",
			Object: &WriteObject{
				Collection: "testcollection",
				Key:        "item_b",
				Value:      `{"foo":"baz"}`,
			},
		},
		&OpWrite{
			OwnerID: "player_003",
			Object: &WriteObject{
				Collection: "testcollection",
				Key:        "item_c",
				Value:      `{"foo":"qux"}`,
			},
		},
	}

	acks, err := WriteObjects(context.Background(), logger, coll, ops)

	if err != nil {
		t.Fatalf("写入失败: %v", err)
	}
	if len(acks) != 3 {
		t.Fatalf("acks 长度期望 3, 实际 %d", len(acks))
	}

	for i, ack := range acks {
		expected := computeVersion(ops[i].Object.Value)
		if ack.Version != expected {
			t.Errorf("ack[%d] version 不匹配, 期望 %s, 实际 %s", i, expected, ack.Version)
		}
	}
}

func TestWriteUpsertOverwrite(t *testing.T) {
	logger, coll, cleanup := testSetup(t)
	defer cleanup()

	ctx := context.Background()

	// 首次写入
	ops := OpWrites{
		&OpWrite{
			OwnerID: "player_001",
			Object: &WriteObject{
				Collection: "testcollection",
				Key:        "item_01",
				Value:      `{"level":1}`,
			},
		},
	}

	acks1, err := WriteObjects(ctx, logger, coll, ops)
	if err != nil {
		t.Fatalf("首次写入失败: %v", err)
	}

	// 不带版本号覆盖写入
	ops[0].Object.Value = `{"level":2}`
	acks2, err := WriteObjects(ctx, logger, coll, ops)
	if err != nil {
		t.Fatalf("覆盖写入失败: %v", err)
	}

	if acks1[0].Version == acks2[0].Version {
		t.Error("覆盖写入后 version 应该不同")
	}

	expectedVersion := computeVersion(`{"level":2}`)
	if acks2[0].Version != expectedVersion {
		t.Errorf("version 不匹配, 期望 %s, 实际 %s", expectedVersion, acks2[0].Version)
	}
}

// --- 乐观锁 (OCC) 测试 ---

func TestWriteVersionMatchSuccess(t *testing.T) {
	logger, coll, cleanup := testSetup(t)
	defer cleanup()

	ctx := context.Background()

	// 先写入一个对象
	ops := OpWrites{
		&OpWrite{
			OwnerID: "player_001",
			Object: &WriteObject{
				Collection: "testcollection",
				Key:        "item_01",
				Value:      `{"foo":"bar"}`,
			},
		},
	}

	acks, err := WriteObjects(ctx, logger, coll, ops)
	if err != nil {
		t.Fatalf("首次写入失败: %v", err)
	}

	// 使用正确版本号更新
	ops[0].Object.Value = `{"foo":"baz"}`
	ops[0].Object.Version = acks[0].Version

	acks2, err := WriteObjects(ctx, logger, coll, ops)
	if err != nil {
		t.Fatalf("版本匹配更新失败: %v", err)
	}

	expectedVersion := computeVersion(`{"foo":"baz"}`)
	if acks2[0].Version != expectedVersion {
		t.Errorf("version 不匹配, 期望 %s, 实际 %s", expectedVersion, acks2[0].Version)
	}
}

func TestWriteVersionMatchFail(t *testing.T) {
	logger, coll, cleanup := testSetup(t)
	defer cleanup()

	ctx := context.Background()

	// 先写入一个对象
	ops := OpWrites{
		&OpWrite{
			OwnerID: "player_001",
			Object: &WriteObject{
				Collection: "testcollection",
				Key:        "item_01",
				Value:      `{"foo":"bar"}`,
			},
		},
	}

	_, err := WriteObjects(ctx, logger, coll, ops)
	if err != nil {
		t.Fatalf("首次写入失败: %v", err)
	}

	// 使用错误版本号更新, 应该失败
	ops[0].Object.Value = `{"foo":"baz"}`
	ops[0].Object.Version = "wrong_version"

	_, err = WriteObjects(ctx, logger, coll, ops)
	if err != ErrRejectedVersion {
		t.Errorf("期望 ErrRejectedVersion, 实际 %v", err)
	}
}

func TestWriteVersionMatchNotExists(t *testing.T) {
	logger, coll, cleanup := testSetup(t)
	defer cleanup()

	// 对象不存在时带版本号写入, 应该失败
	ops := OpWrites{
		&OpWrite{
			OwnerID: "player_001",
			Object: &WriteObject{
				Collection: "testcollection",
				Key:        "nonexistent",
				Value:      `{"foo":"bar"}`,
				Version:    "some_version",
			},
		},
	}

	_, err := WriteObjects(context.Background(), logger, coll, ops)
	if err != ErrRejectedVersion {
		t.Errorf("期望 ErrRejectedVersion, 实际 %v", err)
	}
}

func TestWriteVersionOutdatedFail(t *testing.T) {
	logger, coll, cleanup := testSetup(t)
	defer cleanup()

	ctx := context.Background()

	// 创建对象
	ops := OpWrites{
		&OpWrite{
			OwnerID: "player_001",
			Object: &WriteObject{
				Collection: "testcollection",
				Key:        "item_01",
				Value:      `{"closed":false}`,
			},
		},
	}

	acks, err := WriteObjects(ctx, logger, coll, ops)
	if err != nil {
		t.Fatalf("创建失败: %v", err)
	}

	// 用正确版本号更新
	firstVersion := acks[0].Version
	ops[0].Object.Version = firstVersion
	ops[0].Object.Value = `{"closed":true}`

	_, err = WriteObjects(ctx, logger, coll, ops)
	if err != nil {
		t.Fatalf("第二次写入失败: %v", err)
	}

	// 用旧版本号再次写入, 应该失败
	ops[0].Object.Version = firstVersion
	ops[0].Object.Value = `{"closed":false}`

	_, err = WriteObjects(ctx, logger, coll, ops)
	if err != ErrRejectedVersion {
		t.Errorf("期望 ErrRejectedVersion, 实际 %v", err)
	}
}

func TestWriteVersionCorrectAfterUpdate(t *testing.T) {
	logger, coll, cleanup := testSetup(t)
	defer cleanup()

	ctx := context.Background()

	// 创建对象
	ops := OpWrites{
		&OpWrite{
			OwnerID: "player_001",
			Object: &WriteObject{
				Collection: "testcollection",
				Key:        "item_01",
				Value:      `{"closed":false}`,
			},
		},
	}

	acks, err := WriteObjects(ctx, logger, coll, ops)
	if err != nil {
		t.Fatalf("创建失败: %v", err)
	}

	// 更新
	ops[0].Object.Version = acks[0].Version
	ops[0].Object.Value = `{"closed":true}`

	acks, err = WriteObjects(ctx, logger, coll, ops)
	if err != nil {
		t.Fatalf("第二次写入失败: %v", err)
	}

	// 用最新版本号再次写入, 应该成功
	ops[0].Object.Version = acks[0].Version

	acks, err = WriteObjects(ctx, logger, coll, ops)
	if err != nil {
		t.Errorf("用最新版本写入失败: %v", err)
	}
}

// --- Read 测试 ---

func TestReadSingle(t *testing.T) {
	logger, coll, cleanup := testSetup(t)
	defer cleanup()

	ctx := context.Background()

	// 先写入
	ops := OpWrites{
		&OpWrite{
			OwnerID: "player_001",
			Object: &WriteObject{
				Collection: "testcollection",
				Key:        "item_01",
				Value:      `{"foo":"bar"}`,
			},
		},
	}

	acks, err := WriteObjects(ctx, logger, coll, ops)
	if err != nil {
		t.Fatalf("写入失败: %v", err)
	}

	// 读取
	ids := []*ReadObjectID{
		{Collection: "testcollection", Key: "item_01", PlayerID: "player_001"},
	}
	objects, err := ReadObjects(ctx, logger, coll, ids)
	if err != nil {
		t.Fatalf("读取失败: %v", err)
	}
	if len(objects) != 1 {
		t.Fatalf("objects 长度期望 1, 实际 %d", len(objects))
	}
	if objects[0].Collection != "testcollection" {
		t.Errorf("collection 不匹配")
	}
	if objects[0].Key != "item_01" {
		t.Errorf("key 不匹配")
	}
	if objects[0].PlayerID != "player_001" {
		t.Errorf("player_id 不匹配")
	}
	if objects[0].Value != `{"foo":"bar"}` {
		t.Errorf("value 不匹配, 期望 %s, 实际 %s", `{"foo":"bar"}`, objects[0].Value)
	}
	if objects[0].Version != acks[0].Version {
		t.Errorf("version 不匹配")
	}
}

func TestReadNotFound(t *testing.T) {
	logger, coll, cleanup := testSetup(t)
	defer cleanup()

	ids := []*ReadObjectID{
		{Collection: "testcollection", Key: "nonexistent", PlayerID: "player_001"},
	}
	objects, err := ReadObjects(context.Background(), logger, coll, ids)
	if err != nil {
		t.Fatalf("读取失败: %v", err)
	}
	if len(objects) != 0 {
		t.Errorf("期望空列表, 实际长度 %d", len(objects))
	}
}

func TestReadMixed(t *testing.T) {
	logger, coll, cleanup := testSetup(t)
	defer cleanup()

	ctx := context.Background()

	// 写入一个对象
	ops := OpWrites{
		&OpWrite{
			OwnerID: "player_001",
			Object: &WriteObject{
				Collection: "testcollection",
				Key:        "exists",
				Value:      `{"foo":"bar"}`,
			},
		},
	}
	_, err := WriteObjects(ctx, logger, coll, ops)
	if err != nil {
		t.Fatalf("写入失败: %v", err)
	}

	// 同时读取存在和不存在的对象
	ids := []*ReadObjectID{
		{Collection: "testcollection", Key: "exists", PlayerID: "player_001"},
		{Collection: "testcollection", Key: "notfound", PlayerID: "player_001"},
	}
	objects, err := ReadObjects(ctx, logger, coll, ids)
	if err != nil {
		t.Fatalf("读取失败: %v", err)
	}
	if len(objects) != 1 {
		t.Errorf("期望长度 1, 实际 %d", len(objects))
	}
}

func TestReadEmpty(t *testing.T) {
	logger, coll, cleanup := testSetup(t)
	defer cleanup()

	objects, err := ReadObjects(context.Background(), logger, coll, []*ReadObjectID{})
	if err != nil {
		t.Fatalf("读取失败: %v", err)
	}
	if len(objects) != 0 {
		t.Errorf("期望空列表, 实际长度 %d", len(objects))
	}
}

// --- Delete 测试 ---

func TestDeleteSingle(t *testing.T) {
	logger, coll, cleanup := testSetup(t)
	defer cleanup()

	ctx := context.Background()

	// 写入
	ops := OpWrites{
		&OpWrite{
			OwnerID: "player_001",
			Object: &WriteObject{
				Collection: "testcollection",
				Key:        "item_01",
				Value:      `{"foo":"bar"}`,
			},
		},
	}
	_, err := WriteObjects(ctx, logger, coll, ops)
	if err != nil {
		t.Fatalf("写入失败: %v", err)
	}

	// 删除 (不带版本号)
	deleteOps := OpDeletes{
		&OpDelete{
			OwnerID: "player_001",
			ObjectID: &DeleteObjectID{
				Collection: "testcollection",
				Key:        "item_01",
			},
		},
	}
	err = DeleteObjects(ctx, logger, coll, deleteOps)
	if err != nil {
		t.Fatalf("删除失败: %v", err)
	}

	// 确认已删除
	ids := []*ReadObjectID{
		{Collection: "testcollection", Key: "item_01", PlayerID: "player_001"},
	}
	objects, err := ReadObjects(ctx, logger, coll, ids)
	if err != nil {
		t.Fatalf("读取失败: %v", err)
	}
	if len(objects) != 0 {
		t.Errorf("删除后仍能读到对象")
	}
}

func TestDeleteWithVersionMatch(t *testing.T) {
	logger, coll, cleanup := testSetup(t)
	defer cleanup()

	ctx := context.Background()

	// 写入
	ops := OpWrites{
		&OpWrite{
			OwnerID: "player_001",
			Object: &WriteObject{
				Collection: "testcollection",
				Key:        "item_01",
				Value:      `{"foo":"bar"}`,
			},
		},
	}
	acks, err := WriteObjects(ctx, logger, coll, ops)
	if err != nil {
		t.Fatalf("写入失败: %v", err)
	}

	// 使用正确版本号删除
	deleteOps := OpDeletes{
		&OpDelete{
			OwnerID: "player_001",
			ObjectID: &DeleteObjectID{
				Collection: "testcollection",
				Key:        "item_01",
				Version:    acks[0].Version,
			},
		},
	}
	err = DeleteObjects(ctx, logger, coll, deleteOps)
	if err != nil {
		t.Fatalf("版本匹配删除失败: %v", err)
	}
}

func TestDeleteWithVersionMismatch(t *testing.T) {
	logger, coll, cleanup := testSetup(t)
	defer cleanup()

	ctx := context.Background()

	// 写入
	ops := OpWrites{
		&OpWrite{
			OwnerID: "player_001",
			Object: &WriteObject{
				Collection: "testcollection",
				Key:        "item_01",
				Value:      `{"foo":"bar"}`,
			},
		},
	}
	_, err := WriteObjects(ctx, logger, coll, ops)
	if err != nil {
		t.Fatalf("写入失败: %v", err)
	}

	// 使用错误版本号删除, 应该失败
	deleteOps := OpDeletes{
		&OpDelete{
			OwnerID: "player_001",
			ObjectID: &DeleteObjectID{
				Collection: "testcollection",
				Key:        "item_01",
				Version:    "wrong_version",
			},
		},
	}
	err = DeleteObjects(ctx, logger, coll, deleteOps)
	if err != ErrNotFound {
		t.Errorf("期望 ErrNotFound, 实际 %v", err)
	}
}

func TestDeleteNotExistsNoVersion(t *testing.T) {
	logger, coll, cleanup := testSetup(t)
	defer cleanup()

	// 删除不存在的对象 (不带版本号), 应该幂等成功
	deleteOps := OpDeletes{
		&OpDelete{
			OwnerID: "player_001",
			ObjectID: &DeleteObjectID{
				Collection: "testcollection",
				Key:        "nonexistent",
			},
		},
	}
	err := DeleteObjects(context.Background(), logger, coll, deleteOps)
	if err != nil {
		t.Errorf("不带版本号删除不存在的对象应该成功, 实际 %v", err)
	}
}

func TestDeleteNotExistsWithVersion(t *testing.T) {
	logger, coll, cleanup := testSetup(t)
	defer cleanup()

	// 删除不存在的对象 (带版本号), 应该返回 ErrNotFound
	deleteOps := OpDeletes{
		&OpDelete{
			OwnerID: "player_001",
			ObjectID: &DeleteObjectID{
				Collection: "testcollection",
				Key:        "nonexistent",
				Version:    "some_version",
			},
		},
	}
	err := DeleteObjects(context.Background(), logger, coll, deleteOps)
	if err != ErrNotFound {
		t.Errorf("期望 ErrNotFound, 实际 %v", err)
	}
}

// --- List 测试 ---

func TestListByOwner(t *testing.T) {
	logger, coll, cleanup := testSetup(t)
	defer cleanup()

	ctx := context.Background()

	ops := OpWrites{
		&OpWrite{
			OwnerID: "player_001",
			Object:  &WriteObject{Collection: "inventory", Key: "b", Value: `{}`},
		},
		&OpWrite{
			OwnerID: "player_001",
			Object:  &WriteObject{Collection: "inventory", Key: "a", Value: `{}`},
		},
		&OpWrite{
			OwnerID: "player_001",
			Object:  &WriteObject{Collection: "inventory", Key: "c", Value: `{}`},
		},
	}

	_, err := WriteObjects(ctx, logger, coll, ops)
	if err != nil {
		t.Fatalf("写入失败: %v", err)
	}

	list, err := ListObjects(ctx, logger, coll, "player_001", "inventory", 10, "")
	if err != nil {
		t.Fatalf("列表查询失败: %v", err)
	}
	if len(list.Objects) != 3 {
		t.Errorf("期望 3 个对象, 实际 %d", len(list.Objects))
	}
	if list.Cursor != "" {
		t.Errorf("不应该有下一页游标")
	}
}

func TestListPagination(t *testing.T) {
	logger, coll, cleanup := testSetup(t)
	defer cleanup()

	ctx := context.Background()

	// 写入 5 个对象
	ops := make(OpWrites, 5)
	for i := range ops {
		ops[i] = &OpWrite{
			OwnerID: "player_001",
			Object: &WriteObject{
				Collection: "inventory",
				Key:        fmt.Sprintf("item_%02d", i+1),
				Value:      `{}`,
			},
		}
	}

	_, err := WriteObjects(ctx, logger, coll, ops)
	if err != nil {
		t.Fatalf("写入失败: %v", err)
	}

	// 第一页: 取 2 条
	page1, err := ListObjects(ctx, logger, coll, "player_001", "inventory", 2, "")
	if err != nil {
		t.Fatalf("第一页查询失败: %v", err)
	}
	if len(page1.Objects) != 2 {
		t.Fatalf("第一页期望 2 个对象, 实际 %d", len(page1.Objects))
	}
	if page1.Cursor == "" {
		t.Fatal("第一页应该有游标")
	}

	// 第二页
	page2, err := ListObjects(ctx, logger, coll, "player_001", "inventory", 2, page1.Cursor)
	if err != nil {
		t.Fatalf("第二页查询失败: %v", err)
	}
	if len(page2.Objects) != 2 {
		t.Fatalf("第二页期望 2 个对象, 实际 %d", len(page2.Objects))
	}
	if page2.Cursor == "" {
		t.Fatal("第二页应该有游标")
	}

	// 第三页: 最后 1 条
	page3, err := ListObjects(ctx, logger, coll, "player_001", "inventory", 2, page2.Cursor)
	if err != nil {
		t.Fatalf("第三页查询失败: %v", err)
	}
	if len(page3.Objects) != 1 {
		t.Fatalf("第三页期望 1 个对象, 实际 %d", len(page3.Objects))
	}
	if page3.Cursor != "" {
		t.Errorf("最后一页不应该有游标")
	}

	// 验证三页没有重复
	seen := make(map[string]bool)
	allPages := [][]*Object{page1.Objects, page2.Objects, page3.Objects}
	for _, page := range allPages {
		for _, obj := range page {
			if seen[obj.Key] {
				t.Errorf("key %s 重复出现", obj.Key)
			}
			seen[obj.Key] = true
		}
	}
	if len(seen) != 5 {
		t.Errorf("期望 5 个不重复的 key, 实际 %d", len(seen))
	}
}

func TestListEmpty(t *testing.T) {
	logger, coll, cleanup := testSetup(t)
	defer cleanup()

	list, err := ListObjects(context.Background(), logger, coll, "player_001", "inventory", 10, "")
	if err != nil {
		t.Fatalf("列表查询失败: %v", err)
	}
	if len(list.Objects) != 0 {
		t.Errorf("期望空列表, 实际长度 %d", len(list.Objects))
	}
}

// --- Write + Read 回写验证 ---

func TestWriteThenRead(t *testing.T) {
	logger, coll, cleanup := testSetup(t)
	defer cleanup()

	ctx := context.Background()

	value := `{"name":"烈焰剑","damage":320}`
	ops := OpWrites{
		&OpWrite{
			OwnerID: "player_001",
			Object: &WriteObject{
				Collection: "equipment",
				Key:        "sword_01",
				Value:      value,
			},
		},
	}

	acks, err := WriteObjects(ctx, logger, coll, ops)
	if err != nil {
		t.Fatalf("写入失败: %v", err)
	}

	ids := []*ReadObjectID{
		{Collection: "equipment", Key: "sword_01", PlayerID: "player_001"},
	}
	objects, err := ReadObjects(ctx, logger, coll, ids)
	if err != nil {
		t.Fatalf("读取失败: %v", err)
	}

	if len(objects) != 1 {
		t.Fatalf("期望读到 1 个对象, 实际 %d", len(objects))
	}

	obj := objects[0]
	if obj.Collection != "equipment" {
		t.Errorf("collection 不匹配")
	}
	if obj.Key != "sword_01" {
		t.Errorf("key 不匹配")
	}
	if obj.PlayerID != "player_001" {
		t.Errorf("player_id 不匹配")
	}
	if obj.Value != value {
		t.Errorf("value 不匹配")
	}
	if obj.Version != acks[0].Version {
		t.Errorf("version 不匹配")
	}
	if obj.CreateTime.IsZero() {
		t.Errorf("create_time 不应为零值")
	}
	if obj.UpdateTime.IsZero() {
		t.Errorf("update_time 不应为零值")
	}
}

// --- 工具函数测试 ---

func TestComputeVersion(t *testing.T) {
	value := `{"foo":"bar"}`
	hash := sha256.Sum256([]byte(value))
	expected := hex.EncodeToString(hash[:])

	got := computeVersion(value)
	if got != expected {
		t.Errorf("computeVersion 不匹配, 期望 %s, 实际 %s", expected, got)
	}
}

// --- 边界条件 ---

func TestWriteEmpty(t *testing.T) {
	logger, coll, cleanup := testSetup(t)
	defer cleanup()

	acks, err := WriteObjects(context.Background(), logger, coll, OpWrites{})
	if err != nil {
		t.Fatalf("空写入失败: %v", err)
	}
	if len(acks) != 0 {
		t.Errorf("空写入期望空 acks, 实际长度 %d", len(acks))
	}
}

func TestDeleteEmpty(t *testing.T) {
	logger, coll, cleanup := testSetup(t)
	defer cleanup()

	err := DeleteObjects(context.Background(), logger, coll, OpDeletes{})
	if err != nil {
		t.Errorf("空删除失败: %v", err)
	}
}

func TestWriteSameKeySameOwnerOverwrite(t *testing.T) {
	logger, coll, cleanup := testSetup(t)
	defer cleanup()

	ctx := context.Background()
	collection := "testcollection"
	key := "same_key"

	// 写入 v1
	ops := OpWrites{
		&OpWrite{
			OwnerID: "player_001",
			Object:  &WriteObject{Collection: collection, Key: key, Value: `{"v":1}`},
		},
	}
	_, err := WriteObjects(ctx, logger, coll, ops)
	if err != nil {
		t.Fatalf("写入 v1 失败: %v", err)
	}

	// 同 key 同 owner 写入 v2
	ops[0].Object.Value = `{"v":2}`
	acks, err := WriteObjects(ctx, logger, coll, ops)
	if err != nil {
		t.Fatalf("写入 v2 失败: %v", err)
	}

	// 确认只有一条记录, 值已更新
	var count int64
	count, err = coll.CountDocuments(ctx, bson.M{
		"collection": collection,
		"key":        key,
		"player_id":  "player_001",
	})
	if err != nil {
		t.Fatalf("计数失败: %v", err)
	}
	if count != 1 {
		t.Errorf("期望 1 条记录, 实际 %d", count)
	}

	expectedVersion := computeVersion(`{"v":2}`)
	if acks[0].Version != expectedVersion {
		t.Errorf("version 不匹配, 期望 %s, 实际 %s", expectedVersion, acks[0].Version)
	}
}
