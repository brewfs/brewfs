#!/bin/bash
# 修 wrapper：去掉写死的 -m gpt-6-astra（改用 config 里的 deepseek-flash），再重启
set -u
cat > /root/run-gbrew.sh <<'WRAP'
#!/bin/bash
set -u
export HOME=/root
cd /root/ax-work || exit 1
exec codex exec --sandbox danger-full-access --cd /root/ax-work \
  -c model_reasoning_effort=high \
  "$(cat /root/task-brew-c3-cont.md)" </dev/null
WRAP
chmod +x /root/run-gbrew.sh
echo "=== 新 wrapper ==="
sed 's/^/  /' /root/run-gbrew.sh

tmux kill-session -t gbrew 2>/dev/null || true
pkill -f 'codex exec' 2>/dev/null || true
sleep 2
: > /root/gbrew.log
tmux new-session -d -s gbrew -c /root/ax-work "/root/run-gbrew.sh >> /root/gbrew.log 2>&1"
sleep 50
echo
echo "=== 状态 ==="
echo "  会话: $(tmux ls 2>/dev/null | tr '\n' ' ')"
A=$(stat -c%s /root/gbrew.log 2>/dev/null || echo 0); sleep 10; B=$(stat -c%s /root/gbrew.log 2>/dev/null || echo 0)
echo "  日志: $A -> $B B（$( [ "${B:-0}" -gt "${A:-0}" ] && echo 推进 || echo 静止)）"
echo "  codex 进程: $(pgrep -fc 'codex exec' || echo 0)"
tail -5 /root/gbrew.log 2>/dev/null | cut -c1-130 | sed 's/^/    /'
