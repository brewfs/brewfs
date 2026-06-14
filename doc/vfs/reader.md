# 读路径

## 1. 总览

`src/vfs/io/reader.rs` 负责把“按文件偏移读取”转成一系列可复用的 slice 缓存操作，并在需要时从后端异步拉取数据。

在 BrewFS 中，reader 不直接理解目录或路径，它只处理：

- 某个已知 inode
- 某个偏移范围
- 对应的块布局和 slice 元数据

因此它更像一个“文件数据读取引擎”。

一条典型读路径如下：

```text
VFS / FileHandle::read
  -> FileReader::read_at
  -> 切分为若干 ChunkSpan
  -> 为每段准备 slice
  -> 异步等待 slice 就绪
  -> 拷贝到用户缓冲区
  -> 清理无效缓存
```

## 2. 两层对象

reader 体系分成两层：

- `DataReader`
  - inode 级 reader 注册表
  - 负责按 inode 找到或创建 `FileReader`
  - 负责批量失效
- `FileReader`
  - 单文件读缓存与读路径主体
  - 真正执行 `read_at`

可以把它理解为：

- `DataReader` 管理“有哪些文件正在被读”
- `FileReader` 管理“某一个文件如何被读”

## 3. `DataReader`

`DataReader` 是 VFS 状态中的全局读管理器。

它主要负责：

- 为某个 inode `ensure_file(...)`
- 收集某个 inode 对应的所有 `FileReader`
- 执行 `invalidate(...)`
- 执行 `invalidate_all(...)`

它之所以存在，是因为：

- 同一个 inode 可能同时被多个 handle 读取
- writer 提交新数据后，需要通知所有 reader 失效相应缓存

所以 `DataReader` 的主要角色是“reader 索引器”和“缓存失效广播器”。

## 4. `FileReader`

`FileReader` 是读路径的真正核心对象。

它内部主要包含：

- `config`
- `buffer_usage`
- `inode`
- `slices`
- `sessions`
- `backend`

其中：

- `inode`
  - 提供当前文件本地可见大小
- `slices`
  - 维护本文件已缓存或已排队读取的 slice
- `sessions`
  - 追踪当前句柄的读模式，用于预读和淘汰决策
- `buffer_usage`
  - 参与 reader 内存 back-pressure
- `backend`
  - 用于真正从元数据和块存储读取数据

## 5. reader 中的 slice

reader 自己也维护一套 slice，但它和 writer 的 slice 完全不是一回事。

reader slice 更接近：

- 一段已经读取或正在读取的文件区间缓存
- 包含字节页、状态、等待通知和引用计数

它的状态机比较简单：

- `New`
  - slice 刚创建，还没开始抓取
- `Busy`
  - 后台正在 fetch
- `Ready`
  - 数据已就绪，可读取
- `Invalid`
  - 数据过期，需要淘汰或刷新
- `Refresh`
  - 正在刷新数据

这套状态机的重点不是“写入生命周期”，而是“缓存是否可读”。

## 6. Session 机制

reader 的一个很重要设计是 `Session`。

### 6.1 它解决什么问题

如果 reader 每次只按当前请求精确读取，那么：

- 顺序读取无法预读
- 交错 `pread` 模式命中率很差
- slice 淘汰时缺少“哪些缓存更有用”的依据

所以 reader 需要记录最近读模式，并基于局部性做预测。

### 6.2 Session 中记录什么

一个 `Session` 维护 4 个核心字段：

- `ahead`
  - 预读窗口长度
- `last_off`
  - 最近读取末尾位置
- `total`
  - 当前会话累计顺序读取量
- `atime`
  - 最近访问时间

### 6.3 为什么是两个 Session

每个 `FileReader` 默认维护两个独立 Session。

这样做是为了支持：

- 两条交错顺序流
- 或一次主顺序读取 + 一次局部 seek

如果只维护一个 Session，两个模式会互相污染，导致：

- readahead 预测不稳定
- 无用 slice 不容易识别

两个 Session 让 reader 可以更稳地处理 interleaved `pread`。

### 6.4 Session 如何选择

reader 会按以下顺序选择 Session：

1. 优先找能匹配“向前读取模式”的 Session
2. 再找能匹配“小范围回退读取”的 Session
3. 如果都不匹配，就复用最旧的 Session

这是一种很实用的启发式，而不是精确算法。

目标不是绝对最优，而是用很低成本得到稳定可用的预读行为。

## 7. `read_at()` 主流程

`FileReader::read_at()` 是读路径的核心。

它的主流程可以分成 8 步。

### 7.1 边界裁剪

reader 先基于 `inode.file_size()` 做裁剪：

- 如果 offset 已经在文件尾之后，直接返回 0
- 如果请求超出文件尾，只读取实际可读长度

因此 reader 的文件大小视图来自 VFS 本地 `Inode`，不需要每次都访问元数据层。

### 7.2 清理可淘汰 slice

在真正读取前，reader 会先执行 `clean_evictable_slices()`。

它结合几个因素做淘汰判断：

- 当前请求是否覆盖该 slice
- Session 预测窗口是否仍认为该 slice 有用
- 最近访问时间是否过旧
- slice 是否仍被 pin
- slice 是否仍在飞行中

这说明 reader 的缓存淘汰不是单纯 LRU，而是：

- 当前请求优先
- Session 局部性优先
- 过期且未被引用的数据可回收

### 7.3 内存 back-pressure

reader 有独立的内存压力控制。

逻辑大致是：

- 低于软限制：直接继续
- 高于软限制：短暂 sleep
- 高于硬限制：持续等待，直到使用量下降
- 超过最大等待时间：返回错误

这保证 reader 不会因为激进预读把内存无限吃掉。

### 7.4 切分请求区间

reader 使用 `split_chunk_spans(...)` 把文件偏移范围切成若干 chunk 内区间。

这样后续处理就能以：

- `chunk index`
- `offset in chunk`
- `len`

为单位组织，而不是直接操作“全文件绝对偏移”。

### 7.5 `prepare_slices()`

对每个请求 span，reader 都会调用 `prepare_slices()`。

这一步会：

- 检查当前已有 slice 是否覆盖请求区间
- 对覆盖到的 slice 增加引用计数 `refs`
- 对缺失区间创建新 slice
- 为新 slice 启动后台 fetch 任务

注意这里的关键点：

- `prepare_slices()` 不会等待数据立刻准备好
- 它做的是“确保这段区间最终有 slice 可用”

因此它更像是一次 reservation。

### 7.6 计算预读窗口

reader 接着通过 `check_session()` 更新 Session，并得到当前的 `ahead` 长度。

随后 `prepare_ahead_slices()` 会把当前读请求后面的潜在顺序范围也准备好。

这里体现了 reader 的两个核心目标：

- 当前请求必须读到
- 后续高概率请求尽量提前准备

### 7.7 `read_from_slice()`

真正拷贝数据发生在 `read_from_slice()`。

它会：

1. 找出当前 chunk 对应的所有 slice
2. 过滤与目标区间有交集的 slice
3. 对每个有交集的 slice 调 `wait_ready()`
4. 在就绪后，把对应区间拷贝到目标缓冲区

这里的关键点是：

- slice 就绪前不会忙等，而是挂在 `Notify`
- 只在需要时等待具体 slice
- 数据拷贝按交集区间进行，不要求整个 chunk 一次性齐全

### 7.8 清理 invalid slice

读完成后，reader 会再做一次 `cleanup_invalid()`。

这一步让失效缓存不会长期堆积。

## 8. `wait_ready()`

`wait_ready()` 是 reader 正确性的关键小函数。

它负责等待某个 slice 从：

- `New`
- `Busy`
- `Refresh`

转到：

- `Ready`

或者失败地转到：

- `Invalid`

等待方式不是轮询状态，而是：

- 先检查状态
- 不 ready 时拿到 `Notify`
- `await` 通知
- 被唤醒后再次检查

这保证了 reader 可以安全地和后台 fetch 任务协作。

## 9. 后台 fetch

reader 新建 slice 后，会调用 `SliceState::background_fetch(...)`。

这个后台任务会：

1. 把 slice 状态改成 `Busy`
2. 根据 inode + chunk index 算出 chunk id
3. 从后端读取对应区间数据
4. 成功则写入 `page` 并标记 `Ready`
5. 失败则标记 `Invalid`
6. 最后 `notify_waiters()`

因此 reader 本质上是“前台组织 + 后台拉取”的模式。

前台负责决定读什么，后台负责把数据拉回来。

## 10. `SlicePinGuard`

reader 里有一个很实用的辅助对象：`SlicePinGuard`。

它的作用是：

- 在一次读取期间 pin 住相关 slice
- 读结束时自动减少 `refs`

这让 reader 能在异步读期间安全地防止 slice 被淘汰。

它属于典型的 RAII 用法：

- 进入读取时持有
- 离开作用域自动释放

## 11. 与 writer 的交互

reader 和 writer 不是独立系统，它们通过缓存失效和 dirty overlay 协作。

### 11.1 writer 提交后失效 reader 缓存

当 writer 的 `commit_chunk` 成功后，会通过 `DataReader::invalidate(...)` 把对应读缓存标记为 stale。

这防止 reader 长时间持有旧数据。

### 11.2 VFS 读前可能要求 writer 先可见

在更高层的 VFS 路径里，读之前可能会先让 pending writer 数据变得可见。

也就是说：

- reader 自己负责缓存和 fetch
- 但“是否应该先让 writer 的未提交数据稳定下来”由 VFS 协调

这也是为什么 reader 文档必须和 writer / VFS 文档一起看。

## 12. 为什么 reader 不直接读整个文件

reader 采用 slice 化和按需加载，而不是粗暴地整个文件缓存，原因主要有 4 点：

- 文件可能非常大
- 访问模式通常是局部的
- 顺序流和随机流混合时需要灵活预读
- 需要跟 writer 的局部失效配合

因此它的核心策略是：

- 小粒度缓存
- 弱状态机
- Session 驱动的预读
- 显式失效与淘汰

## 13. 设计收益

当前 reader 设计带来几个明显收益：

- 支持顺序读的自适应预读
- 支持交错 `pread` 的双 Session 模式
- 读缓存和 writer 提交之间有明确失效边界
- 能在内存受限时主动 back-pressure
- 用 pin + notify 让异步 fetch 与前台读取协作清晰

总结起来，reader 不是一个“简单缓存层”，而是一个：

- 预测访问模式
- 管理小粒度 slice 生命周期
- 协调前台读取与后台 fetch

的读引擎。
