# -*- coding: utf-8 -*-
import io
path = r'C:\Users\HXY\Documents\Codex\2026-10-22\https-www-usenix-org-conferences-author\work\ax-plan-review\runs\dsh-experiment-context-20261106\METRICS.md'
with io.open(path, encoding='utf-8') as f:
    text = f.read()

add = u'''
### C2 django 在 BrewFS 上：未实测，按 pytest 速率外推

```
django trace 有 57,787,980 个事件（pytest 只有 1,747,109，是它的 33×）。
BrewFS 实测速率（pytest）= 1,747,109 / 680.06 s ≈ 2,570 events/s
⇒ django 在 BrewFS 上全量外推 ≈ 57,787,980 / 2,570 ≈ 22,485 s ≈ **6.2 小时**
限时 30 分钟的采样在启动阶段（<65,536 事件，未打印第一条进度）就耗尽了预算，
因此 django on BrewFS **记为 NOT MEASURED**，只报外推值，不写进对比表。
（JuiceFS 侧 django 实测 2,603.9 s / 57,787,980 callbacks 是既有数据。）
```

'''
anchor = u'## C3 — 百万小文件'
idx = text.index(anchor)
text = text[:idx] + add + text[idx:]
with io.open(path, 'w', encoding='utf-8') as f:
    f.write(text)
print('django note added, bytes =', len(text.encode('utf-8')))
