# 量化数据工作台

这个项目现在提供一个现代化、简洁的桌面 GUI，用于在界面中直接触发 AKShare 数据获取并展示为表格。

## 已实现能力

- 现代化桌面界面（`PySide6`）
- GUI 触发数据抓取，异步执行避免卡顿
- 固定深色主题（不跟随系统主题切换）
- 左侧工具栏 + 页面化操作（自选 / 行情 / 数据），右侧集中展示结果
- 左侧显示运行日志，右侧专注结果表格
- 右下角悬浮胶囊：代码输入框与下载进度固定在右下角
- 数据页支持“获取历史数据”开关（勾选后忽略开始/结束日期）
- 行情页支持周期切换（分时/5分/15分/30分/60分/日/周/月）
- 行情页支持 K 线 + 均线 + 成交量 + 指标开关（MACD / KDJ / RSI / BOLL）
- 行情图支持鼠标拖拽平移、滚轮缩放
- 行情图已切换到 `pyqtgraph` 高性能渲染（基于 Qt 图形栈）
- 右侧扩展模块支持盘口五档、逐笔成交、资金流向
- 自选页双击代码可直接切换到行情页并刷新
- 表格表头自动转中文（对英文列名做本地化映射）
- 本地缓存（默认优先使用缓存，按需手动刷新）
- 行情与K线缓存支持增量更新：只下载缺失区间，已有区间直接命中本地
- 支持的数据集：
  - A 股日线（自动多源回退：`stock_zh_a_hist` / `stock_zh_a_daily` / `stock_zh_a_hist_tx`）
  - 指数日线（自动多源回退：`index_zh_a_hist` / `stock_zh_index_daily` / `stock_zh_index_daily_tx`）
  - 深交所市场总貌 (`stock_szse_summary`)
- 抓取结果在 GUI 表格中实时展示，并输出运行日志

## 安装依赖

```bash
pip install -r requirements.txt
```

## 启动 GUI

```bash
python main.py
```

## 项目结构（模块化）

```text
.
├── GUI
│   ├── main.py                     # GUI 入口
│   ├── windows/main_window.py      # 主窗口与交互编排
│   ├── config/datasets.py          # 数据集配置
│   ├── config/column_labels.py     # 表头中文映射
│   ├── styles/dark_theme.py        # 固定深色主题样式
│   ├── services/symbol_service.py  # 股票代码联想与缓存服务
│   ├── widgets/styled_combo_box.py # 自定义下拉组件
│   ├── widgets/market_chart.py     # 行情历史图组件
│   └── workers/fetch_worker.py     # 后台抓取线程
├── core/data_service.py            # 数据抓取编排 + 多源回退
├── core/timeseries_cache.py        # 时序缓存模块（增量范围计算/命中/合并）
├── datafetch/datafetch.py          # 脚本调用示例
└── cache                           # 本地缓存目录
```

## 缓存说明

- 数据缓存目录：`cache/datasets`
- 股票代码联想缓存目录：`cache/symbols`
- 默认优先读取本地缓存，避免重复网络请求
- 如需强制刷新，请在左侧面板勾选 `刷新缓存`
- 启动时会自动将历史遗留缓存目录（如 `GUI/cache`）迁移到项目根 `cache`

## 数据脚本示例

```bash
python -m datafetch.datafetch
```

## 相关文档

- [AKShare Document](https://akshare.akfamily.xyz/introduction.html)
- [AKTools Document](https://aktools.akfamily.xyz)
