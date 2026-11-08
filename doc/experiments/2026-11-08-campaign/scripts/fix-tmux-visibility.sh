#!/bin/bash
# 1) 给正在跑的 g426 开一个可观看的镜像会话  2) 之后的 agent 一律「tmux 里可见 + 可注入」
set -u
echo "=== 1. 诊断：g426 为什么看不到 ==="
echo "  tmux 会话存在：$(tmux ls 2>/dev/null | head -1)"
echo "  启动方式：/home/hxy/run-426.sh > /home/hxy/logs/g426.log 2>&1"
echo "  ⇒ stdout/stderr 被重定向到日志文件，所以 pane 是空的（只能看日志）"

echo
echo "=== 2. 建一个镜像会话 view426（tail -f 实时看）==="
tmux kill-session -t view426 2>/dev/null
tmux new-session -d -s view426 "tail -f /home/hxy/logs/g426.log"
sleep 2
echo "  会话列表："
tmux ls 2>/dev/null | sed 's/^/    /'
echo "  view426 pane 内容（最后 6 行）："
tmux capture-pane -p -t view426 2>/dev/null | tail -6 | cut -c1-130 | sed 's/^/    /'

echo
echo "=== 3. 写「可见 + 可注入」的启动模板（给 #424 用）==="
cat > /home/hxy/launch-agent-visible.sh <<'EOS'
#!/bin/bash
# 用法: launch-agent-visible.sh <会话名> <任务文件> [日志文件]
# 特点：
#   ① 在 tmux 里跑交互式 codex（TUI），可以 tmux attach 实时看
#   ② 同时 tee 到日志，便于我这种编排者抓取
#   ③ 需要中途纠偏时可以直接 tmux send-keys 输入
set -u
SESS=${1:?会话名}
TASK=${2:?任务文件}
LOG=${3:-/home/hxy/logs/$SESS.log}
CO=/home/hxy/.local/share/myaiim-codex-test
BIN=$CO/cli/node_modules/.bin/codex

export PATH=$CO/node-v24.22.0-linux-x64/bin:/home/hxy/shim:/home/hxy/.cargo/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
export HOME=/home/hxy
export CARGO_TARGET_DIR=/home/hxy/brewfs-pkv-target

mkdir -p "$(dirname "$LOG")" /home/hxy/logs
tmux kill-session -t "$SESS" 2>/dev/null

# 关键：不再把输出丢进 /dev/null；用 tee 让 pane 与日志同时可见
tmux new-session -d -s "$SESS" -c /home/hxy/brewfs-main \
  "$BIN --sandbox danger-full-access -m gpt-6-astra -c model_reasoning_effort=max \"\$(cat $TASK)\" 2>&1 | tee $LOG"
echo "  已启动会话 $SESS（日志 $LOG）"
echo "  观看：tmux attach -t $SESS  （或用 capture-pane 抓取）"
echo "  纠偏：tmux send-keys -t $SESS '你的指令' Enter"
EOS
chmod +x /home/hxy/launch-agent-visible.sh
echo "  已写 /home/hxy/launch-agent-visible.sh"

echo
echo "=== 4. 验证 codex 是否支持交互模式 ==="
$CO/cli/node_modules/.bin/codex --help 2>/dev/null | grep -E '^\s+(exec|resume|--sandbox)' | head -6 | sed 's/^/  /'
