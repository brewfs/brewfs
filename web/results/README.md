# BrewFS Result Vault

一个独立的、浏览器本地优先的 BrewFS 测试结果浏览器。它不会把日志自动发送到服务器，上传内容保存在当前浏览器的 IndexedDB 中。

## 开发

```powershell
cd web/results
npm install
npm run dev
```

打开终端打印的地址（默认 `http://127.0.0.1:5174`）。支持：

- 上传 ACK 导出的 `.zip`，或直接选择一个结果目录；
- 保留 ZIP central directory 中的原始修改时间、文件大小、压缩方式和 Unix mode；
- 按跑次名称、Redis/TiKV、S3/local-fs、状态和文件路径查询；
- 查看跑次时间范围、文件清单和 Markdown/日志/TSV/JSON 等文本预览；
- 自动解析 `perf-summary.tsv`、fio JSON 和 fully-drained throughput，展示耗时、吞吐、IOPS 与 p99；
- 在左侧勾选最多四个跑次，按吞吐、IOPS 或耗时绘制可视化对比；
- 将当前跑次重新下载为 ZIP，重新打包时继续写入每个文件的原始 mtime。

`run_aliyun_perf_k8s.ps1` 导出的目录或 `.zip` 可以直接拖到页面中。脚本默认包含 fio-bigwrite、fio-bigread、fio-seqread、fio-seqwrite、fio-randread、fio-randwrite、fio-randrw 以及 dirstress/dirperf/metaperf/looptest；也可以通过 `-PerfTools` 缩小矩阵。浏览器清理站点数据会删除本地结果，因此需要长期保存时请同时保留脚本生成的 ZIP 归档。

## 服务器模式

`server.py` 提供同源静态站点和持久化 API。它把原始 ZIP 保存在 `BREWFS_RESULTS_ROOT`，同时解压出带原始 mtime/mode 的文件树；重启服务不会丢失已上传跑次。

```bash
BREWFS_RESULTS_BIND=0.0.0.0 \
BREWFS_RESULTS_PORT=8080 \
BREWFS_RESULTS_ROOT=/var/lib/brewfs-results \
BREWFS_RESULTS_STATIC=/opt/brewfs-results/dist \
BREWFS_RESULTS_MAX_UPLOAD=67108864 \
BREWFS_RESULTS_MAX_EXTRACTED=4294967296 \
BREWFS_RESULTS_MAX_FILES=100000 \
python3 server.py
```

| 环境变量 | 默认值 | 用途 |
| --- | --- | --- |
| `BREWFS_RESULTS_BIND` | `127.0.0.1` | HTTP 监听地址 |
| `BREWFS_RESULTS_PORT` | `8080` | HTTP 监听端口 |
| `BREWFS_RESULTS_ROOT` | `/var/lib/brewfs-results` | ZIP 与解压结果的持久化目录 |
| `BREWFS_RESULTS_STATIC` | `web/results/dist` | Vite 构建后的静态文件目录 |
| `BREWFS_RESULTS_MAX_UPLOAD` | `67108864` | 单个上传请求的最大字节数；上传体会在内存中解析，应保持合理上限 |
| `BREWFS_RESULTS_MAX_EXTRACTED` | `4294967296` | ZIP 解压后的最大总字节数，用于限制压缩炸弹 |
| `BREWFS_RESULTS_MAX_FILES` | `100000` | ZIP 中允许的最大条目数 |

云端 runner 使用独立的 `BREWFS_RESULTS_URL` 环境变量指定此服务的公开基址，例如 `https://results.example.com`；该值属于上传客户端配置，不需要设置在服务器进程中。

API 为 `GET /api/runs`、`POST /api/runs`（multipart 字段 `archive`）、`GET /api/runs/<id>/files/<path>`、`GET /api/runs/<id>/archive` 和 `DELETE /api/runs/<id>`。生产环境请将实例安全组或反向代理限制为可信网络，并自行增加认证；服务本身不提供用户认证。
