# Polymarket BTC 5m V7 — 运行说明与本次验证

## 启动

```powershell
cd C:\Users\孙奕涵\Desktop\初秒狙击\new-polymarket-trader-main
powershell -ExecutionPolicy Bypass -File .\run.ps1
```

`run.ps1` 会自动：

- 创建 `data/` 和 `logs/`
- 使用包内 `runtime/` 的 Python 依赖
- 读取 `config.yaml`，以 paper 模式启动

也可以手动运行：

```powershell
$env:PYTHONPATH = "$PWD\runtime"
python -u main.py --config config.yaml
```

## 本次修改

已应用 `resolution_settlement.diff`，实际源码已修改，不只是提供补丁。

覆盖文件：

- `main.py`
- `core/feed.py`
- `core/executor.py`
- `core/state_manager.py`
- `config.yaml`

## 回测/冒烟验证（2026-11-04）

使用 `config.smoke.yaml`（仅测试用，未打包；`force_trade_test: true`）跑真实 WebSocket 和 Gamma：

1. 21:45 发现 `btc-updown-5m-1793799900`
2. 21:46:00 强制测试入场：
   - side `DOWN`
   - ask 0.4100，fill 0.4300
   - shares 23.2558
   - cost 10.3990
3. 21:50:00 盘口结束，冻结 `spot_close=78384.005`
4. Gamma 在 120 秒内没有返回 `closed=true`，21:52:00 触发本地兜底
5. 结算结果：

```text
[结算] 盘口 btc-updown-5m-1793799900 结束 | 最终赢家: DOWN | 我的持仓: NO (DOWN) | 最终价格: 1.0000 | 来源: local-spot-close | 份数: 23.2558 | 成本: 10.3990 | 回收: 23.2558 | 盈亏: +12.8568 | 最新余额: 1012.8568 | executor_balance: 1012.8568 spot_open=78391.83499999999 spot_close=78384.005
```

资金校验：

```text
1000.0000 - 10.3990 + 23.2558 = 1012.8568
```

持仓已从 state 中删除。

强制配置在结算后又立即开了下一盘 `btc-updown-5m-1793800200:UP`。我随即停止机器人并删除了 smoke state，因此这次只完成了第一笔的完整“开仓 → 收盘 → 兜底结算 → 余额更新”闭环。

## 生产配置

打包内的 `config.yaml` 仍然保持：

```yaml
mode: "paper"
strategy:
  force_trade_test: false
```

`config.smoke.yaml` 和 `data/smoke_state.json` 只用于本次测试，未放入压缩包。

## 已知事项

- `resolution_settlement.diff` 是本次修改的完整补丁记录，包内源码已经应用过该补丁。
- live 模式没有用真实资金验证；本次只验证 paper。
- Binance 行情在 INFO 级别逐 tick 打印，日志会比较大；这是原有行为，未在本次结算补丁中改动。
