# JuiceFS Slice 压缩 & S3 适配

> 基于 `meta/slice.go`, `meta/base.go`, `pkg/object/s3.go`

## Slice 数据结构 (meta/slice.go:21)

```go
type slice struct {
    id    uint64    // slice 对象存储 ID
    size  uint32    // 压缩后 chunk 总大小
    off   uint32    // 在 chunk 内的偏移
    len   uint32    // 本 slice 逻辑长度
    pos   uint32    // 在文件 chunk 内的位置
    left  *slice    // 二叉树左子 (空洞)
    right *slice    // 二叉树右子
}
```

每个 slice 在 Redis 中占 24 字节: `pos(4) + id(8) + size(4) + off(4) + len(4)`

**关键常量**:
- `sliceBytes = 24`
- `maxSlices = 2500` — 强制压缩阈值
- `maxCompactSlices = 1000` — 单次压缩最大处理切片数

## cut(pos) — 二叉树分裂 (line 55)

在位置 `pos` 将树一分为二，返回 `(left, right)`:

```
case pos <= s.pos:
    左边补 hole (id=0, pos→s.pos-pos) → 递归 cut left → return (newLeft, s)

case pos < s.pos + s.len:
    切片从 pos 切成两半:
      left:  pos=s.pos, len=pos-s.pos
      right: pos=pos,   id=s.id, len=s.len-l, off=s.off+l
    return (s, right)

case pos >= s.pos + s.len:
    右边补 hole (s.pos+s.len→pos-(s.pos+s.len)) → 递归 cut right → return (s, newRight)
```

## buildSlice(ss) — 二叉树合并 (line 134)

对所有切片建立二叉树视图，按位置合并重叠:

```
Algorithm:
  root = nil
  for each input slice s:
    left, _   = root.cut(s.pos)           // 树在 s.pos 处分裂
    _, right  = right.cut(s.pos + s.len)  // 右半部分在 s.pos+s.len 处再分裂
    s.left = left; s.right = right
    root = s                               // s 成为新 root
  in-order traversal (left→self→right):
    空洞补零 → 输出 {Id, Size, Off, Len}
```

**Result**: 排序、空洞填充、去重的完整切片列表。

## compactChunk(ss) (line 157)

```go
func compactChunk(ss []*slice) (pos uint32, size uint32, chunk []Slice)
```

```
1. buildSlice(ss) → 获取完整切片列表
2. 裁剪首部空洞: while chunk[0].Id == 0 { pos += Len; chunk = chunk[1:] }
3. 裁剪尾部空洞: while chunk[n-1].Id == 0 { chunk = chunk[:n-1] }
4. 如果只剩一个空洞 → 设其 len = 1 (占位)
5. 汇总长度 → return (newPos, totalSize, chunk)
```

## skipSome(ss) (line 183)

压缩优化: 跳过头部的大切片，避免重新上传:

```
for each candidate first slice:
  用 compactChunk 尝试压缩
  if first.len < 1MB || first.len*5 < total_size: break
  if 压缩后不再以 first 开头 (被吸收): break
  if 存在重复的 first: break
  skipped++
```

返回可安全跳过的前导切片数量。

## 压缩触发与执行 (base.go:2798)

### 触发条件

| 条件 | 类型 | 行为 |
|------|------|------|
| `numSlices % 100 == 99` | 非强制 | 后台 goroutine, 不阻塞 |
| `numSlices > 350` | 非强制 | 后台 goroutine |
| **`numSlices >= 2500`** | **强制** | **阻塞写入，等待完成** |

### 执行流程

```
1. doRead() → LRANGE c{ino}_{indx} 获取所有切片
2. compactChunk() → 合并为最小非重叠集合
3. NewSlice() → 分配新 ID
4. 后台: Read old slices → merge data → PUT merged object to S3
5. doCompactChunk() → CAS (LTRIM+LPUSH) 原子替换 Redis 列表
6. sliceRefs 引用计数: 旧切片 ref-1, 新切片 ref=0
7. ref < 0 → deleteSlice() → DeleteObject from S3
```

### 并发控制

```go
k := inode + (indx << 40)
m.compacting[k] = true     // dedup: 同一 chunk 只有一个压缩 goroutine
```

- `once || force` → spin-wait 已有压缩完成
- 非强制 + `len(compacting) > 10` → 跳过 (避免并发过多)

---

## S3 对象存储适配 (pkg/object/s3.go)

### Bucket URL 解析 (newS3, line 470)

JuiceFS 的 `--bucket` 支持三种格式:

```
格式 1: http://ENDPOINT/BUCKET       → 自定义 endpoint
格式 2: https://s3-REGION.amazonaws.com/BUCKET  → 标准 S3
格式 3: BUCKET.ENDPOINT              → VPC / 兼容 S3
```

**解析逻辑**:
1. URL 有 path → `[ENDPOINT]/[BUCKET]` 提取
2. URL 无 path → `[BUCKET].[ENDPOINT]` 拆分
3. Region 级联: 提取的 → `AWS_REGION` → `AWS_DEFAULT_REGION` → `us-east-1`
4. 兼容 S3 检测: Oracle OCI, OVH Cloud 等特殊 pattern

### 认证链

```go
accessKey == "anonymous" → AnonymousCredentials
accessKey available   → StaticCredentialsProvider(ak, sk, token)
otherwise             → config.LoadDefaultConfig (env/IAM chain)
```

### 传输优化

- `RetryMaxAttempts = 1` — JuiceFS 自己处理重试逻辑, S3 SDK 不自动重试
- `SwapComputePayloadSHA256ForUnsignedPayloadMiddleware` — 跳过 payload SHA256 签名 (省 CPU)
- `disable-100-continue` query param: `ContinueHeaderThresholdBytes = -1`
- `disable-checksum` query param: 跳过 SDK 层 checksum, JuiceFS 自己管理

### 操作

| Operation | Line | 机制 |
|-----------|------|------|
| `Get()` | 122 | Range GET; 全读时验证 `X-Amz-Meta-Juicefs-Checksum` |
| `Put()` | 155 | 非 seekable body → 读入内存; `RequestEntityTooLarge` → multipart |
| `List()` | 232 | ListObjectsV2, max keys 1000; URL-decode keys |
| `Copy()` | 196 | CopyObject with storage class |
| `Delete()` | 209 | DeleteObject; 忽略 NoSuchKey |
| `Head()` | 99 | HeadObject → Object{size, mtime, ...} |

**Multipart upload fallback** (`putMulti`): 当 PUT 因 body 太大失败时自动切换。
MinPartSize = 5MB, MaxPartSize = 5GB, MaxPartCount = 10000.

---

## 垃圾回收

### Slice 引用计数 (sliceRefs)

- `sliceRefs` 是 Redis Hash: `k{id}_{size} → refcount`
- `NewSlice()` → ref = 0 (尚未被任何 chunk 引用)
- `doWrite()` → RPUSH 到 chunk → 隐式增加引用
- `doCompactChunk()` → 旧切片 ref-1; 新切片 ref=0
- `doDeleteFile()` → 每个切片 ref-1
- ref < 0 → `deleteSlice()` → `storage.Delete(key)` → DEL from `sliceRefs`

### 后台清理 goroutine

| Goroutine | 周期 | 功能 |
|-----------|------|------|
| `cleanupDeletedFiles()` | ~1h | 扫描 `delfiles` ZSet, 删除过期文件 |
| `cleanupSlices()` | ~1h | 扫描 `delSlices`, 清理延迟删除切片 |
| `cleanupTrash()` | ~1h | 扫描 Trash 目录, 清理过期文件 |
| `cleanupLeakedChunks()` | 手动 | 扫描 `c*` keys, 检查对应 inode 是否存在 |
| `cleanupOldSliceRefs()` | 手动 | 清理负值和零值的 `sliceRefs` 条目 |
