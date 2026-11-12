# Packed 005：语义校验与真实源文件验证（2026-10-03）

本轮按 [SPEC 差距清单](../superpowers/plans/2026-10-03-packed-v3-spec-gap-audit.md)
推进 G01、G02–G04：补齐 005 热属性语义拒绝，增加有界 Linux 单文件真实捕获，
并修复实挂载发现的非 UTF-8 名称打开问题。本文记录正确性阶段，不给出性能胜出结论。
完整三创新实验仍按 [当前方案](../superpowers/plans/2026-10-03-brewfs-three-innovations-experiment-plan.md)
的依赖开放。

## 代码与源语义

- GM07 writer/decoder 和 IL05 共用 005 专用校验：inode 在 `1..=i64::MAX`、
  nlink 非零、kind 1–7、kind/mode 类型一致、拒绝未知 mode 高位；rdev 必须适合
  u32，非设备 rdev 为零。GM07 拒绝未知 flags 和非 regular data extents。
  GM05/GM06 的 legacy 004 校验没有被原地收紧。
- `CapturedV3SourceFile` 通过 O_NOFOLLOW/O_NONBLOCK 和 fstat 只接受 regular 文件。
  SEEK_DATA/SEEK_HOLE 捕获实际 data，保留逻辑 EOF；默认 logical ≤64MiB、
  data ≤16MiB、extent records ≤256。超限明确拒绝，没有自动 external-large fallback。
- fd 查询 xattr 保留原始 name bytes 和 binary value，cold 属性总预算 256KiB。
  dev/ino/size/mode/uid/gid/nlink/mtime/ctime/blocks token 与 pathname 复查检测普通
  修改和替换；自己读取导致的 atime 变化不误报。该机制不是原子目录快照协议。
- `packed_v3_snapshot_fixture --wire-version 5 --source-file PATH` 经现有 producer
  上传并认证回读，finish 前后复查源；最后输出 manifest key 和 `.source.json`。
  检测到变化时不写本次输出；先上传的不可达对象仍由调用方负责清理。
- 单 dentry 导入的 visible nlink=1，源 nlink 保存在 provenance。根属性仍由 facade
  合成；源 st_blocks 记录于 provenance，尚未保存为 wire/FUSE 属性。
- 新增 `MetaLayer::get_paths_bytes` 及 VFS 转发。Packed 反向路径和祖先名保留 bytes，
  FUSE inode-based 权限检查据此遍历；UTF-8 路径继续使用原有 lookup 路径。
  String API 仍明确拒绝非法 UTF-8。005 反向输出保留 4,096 aliases、256KiB 与
  1,024 层边界；它不解决 G07 的 mount-wide 预算。

## 失败证据与修复范围

本轮主产物目录：
`docker/compose-xfstests/artifacts/packed-v3-completion-20261003-source/`。
HEAD 为 `a429b0e1bc1c158af06ecf738e552062123d6e00`，所有新增修改仍在原分支工作树。
`initial-source-hashes.json`、最终 source/binary hashes 和逐文件差异用于标识未提交源码；
不能只用 HEAD 推断测试了哪些内容。继承修改及未跟踪 vendor 均保留。

- `gm07-red.log`：1 passed/4 failed，编码和解码分别枚举未知 kind/flags、无效
  identity/mode/rdev/nlink、非文件 extents，证明修复前接受错误语义。
  `gm07-green.log` 及最终 packed suite 包含修复后的回归。
- 认证容器回归重新计算内部 metadata/container/对象哈希，仍要求拒绝 malformed kind。
  IL05 encode/decode 单独覆盖 kind/mode 拒绝，避免 getattr 跳过 GroupMeta 时漏检。
- 首次真实 FUSE 的 raw dense/sparse/all-hole/empty 通过，`source-fuse-raw-4` 的
  非 UTF-8 名称 open 返回 EINVAL。失败日志和成功清理记录保留，后续产物另置
  `after-raw-fix/`，没有覆盖失败证据。
- `raw-open-red.log` 在非 root FUSE open 复现 Errno(22)；`raw-open-green.log`
  证明修复后通过。最终回归同时检查 raw directory/leaf 路径精确字节、String API
  拒绝、允许的 open 和没有祖先搜索权限时的 EACCES，没有绕过权限校验。
- 回归编写期间的编译/fixture 输入错误另外保存在
  `raw-open-test-compile-error.log`、`raw-open-test-setup-error.log`；这些不是行为 red
  证据。最初 source roundtrip 的测试 API 编译错误也保留在 `packed-final-tests.log`。

## 最终验证

raw-name 修复后的最终源码门禁全部通过，位于 `final-gate/`；主目录的初次完整门禁
属于修复前 checkpoint。最终 fmt、必需脚本/报表及 scanner/runner 回归、workspace
check/build、vendor 13 tests 与3 runtimes、BrewFS tokio/io-uring 含/不含 overlay、
fixture build/test、default/overlay clippy、diff check 均 exit0。

| 最终 Rust suite | passed / failed / ignored |
| --- | --- |
| workspace brewfs lib（默认 features） | 1,015 / 0 / 225 |
| workspace brewfs bin（默认 features） | 1,099 / 0 / 225 |
| 完整 brewfs lib，workspace-overlay | 1,299 / 0 / 227 |
| packed feature focused suite | 116 / 0 / 0 |
| overlay focused suite | 273 / 0 / 2 |
| fixture bin | 1 / 0 / 0 |

这些 suite 有重叠，不相加为独立测试数。Clippy 保留既有 warnings；没有声称
all-feature/operator/`-D warnings` GitHub jobs 通过。G01 以本批 red/green、认证链负向
回归、旧004回归和完整同迭代 gate 关闭；G02–G04 仍只接受下面的具体子项。

真实源 LocalFS + FUSE 产物为 `after-raw-fix/source-fuse-summary.json`：

| 真实源类型 | 每种 codec 的完整回读 bytes | raw / zstd | 验证范围 |
| --- | ---: | --- | --- |
| dense，mode 0640，binary user xattr | 1,048,593 | 均通过 | full/partial bytes、size/mode/uid/gid/mtime、xattr、EROFS |
| sparse，8MiB 逻辑 EOF | 8,388,608 | 均通过 | data/hole/跨边界/EOF、full bytes、属性 |
| all-hole，1MiB | 1,048,576 | 均通过 | 全零和 EOF、属性 |
| empty | 0 | 均通过 | 空读、EOF、属性 |
| basename 原始 bytes，hex=`7261772dff` | 15 | 均通过 | 精确原始 readdir name、lookup/open/full/partial bytes |

上述 10 次 errors=0，daemon exit=0，mount removed=true，正常清理。metadata 8MiB，
payload memory/SSD、decoded/window cache 为 0，prefetch off、TTL0、direct I/O、keep-cache0。
没有 drop host page cache，因此只作为 correctness。10 次完整回读共 20,971,584 bytes；
另有局部读取。单文件多次上传/挂载不代表完整目录 inventory 已实现。

RustFS + FUSE 通过既有 `tools/perf/run_packed_local.sh` 执行：wire5、每文件 100KiB、
16 workers、shuffle seed=20261001、full、1 epoch。

| 产物目录（位于 `docker/compose-xfstests/artifacts/`） | 文件 / payload bytes | 结果 |
| --- | ---: | --- |
| `packed-local-20261003T152607Z-2074323/` | 100 / 10,240,000 | errors=0，逐 chunk 比对完整 pattern；cold/readlink/binary xattr/EROFS、跨目录 hardlink inode/nlink=2/content 全通过；cleanup=0 |
| `packed-local-20261003T152723Z-2075249/` | 1,000 / 102,400,000 | errors=0，逐 chunk 比对完整 pattern；cleanup=0 |

均记录成功 host drop-caches、TTL0/direct I/O/keep-cache0、payload memory/SSD/window/
decoded 为 0、data prefetch off，metadata 8MiB/prefetch off；运行 `.stats` data-cache
hits 和 backend failures 为 0。100 文件附加 cold/hardlink 共读取 23 bytes，未计入普通
扫描分母。directory discovery 和附加验证会预先访问 metadata，因此不据此声称
metadata-cold。fixture 默认 zstd/zstd、可压缩 pattern、debug 构建且非匹配 baseline；
仅接受内容与资源清理结果，不接受吞吐优势。

两个 RustFS run 各保存 source/binary hashes、daemon/scanner memory、配置、cache proof、
mount/scan/drain timing 和 teardown 日志；10 次源 FUSE 保存 binary hashes 和逐文件
source provenance。`final-evidence.py` 交叉核对运行源码/二进制身份、零错误与清理，
保存 `final-resource-state.json`。没有创建云资源或 commit/push。

## 仍未关闭的门禁

G02–G04 只推进上述单文件部分：完整 namespace inventory、一致目录视图、根属性、
st_blocks wire、special 文件真实导入、ACL 权限/继承、跨子树 hardlink 策略和 external
large placement 仍待实现。G05–G17 的分页复杂度、完整流量分类、共享预算、native
同 placement/executor 桥、005 pipeline、workspace binding/fence/publication/recovery/GC
和 static/inline/p90 实验控制没有因这次验证而完成。

下一批先推进真实源 inventory/root/blocks 的独立 wire 契约及 source consistency，再补
external-large 和完整观测/预算。主四格消融仍依赖 G08/G15；三创新联合生命周期仍依赖
G10–G13。本轮 debug、可压缩、loopback correctness 行不更新历史性能比较表。
