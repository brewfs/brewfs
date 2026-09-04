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
- 将当前跑次重新下载为 ZIP，重新打包时继续写入每个文件的原始 mtime。

`run_aliyun_perf_k8s.ps1` 导出的目录或 `.zip` 可以直接拖到页面中。浏览器清理站点数据会删除本地结果，因此需要长期保存时请同时保留脚本生成的 ZIP 归档。
