# 量化数据工作台（MoS Quant）

以 `mos_quant/` 为核心包，提供：
- 数据源探测（选择当前网络可用的默认数据源）
- AkShare 数据获取（多源路由与失败回退）
- 本地 DataFrame 缓存（按“新鲜度”自动失效）
- 可选 PySide6 GUI（启动时执行 loader，避免主界面卡顿）

## 快速开始

安装依赖：

```bash
python3 -m pip install -r requirements.txt
```

启动 GUI（兼容入口）：

```bash
python3 main.py
```

不装 GUI 依赖时（仅终端运行 loader）：

```bash
python3 main.py --headless
```

推荐入口（可选）：

```bash
python3 -m mos_quant
```

仅在终端运行 loader（无 GUI）：

```bash
python3 -m mos_quant --headless
```

## 项目结构

```text
.
├── mos_quant/                 # 主包（实现都在这里）
│   ├── caching/               # FileCache / TimeSeriesCache 等缓存工具
│   ├── core/                  # loader/启动编排
│   ├── data/                  # 网络探测、AkShare fetcher
│   └── ui/                    # GUI（可选）
├── config/                    # 用户可编辑配置（会被程序读取）
│   ├── probe_urls.json        # loader 探测 URL 列表
│   └── akshare_sources.json   # AkShare 多源路由配置
├── cache/                     # 运行期缓存/状态（已在 .gitignore 中忽略）
│   └── watch/
│       └── default_data_source.json  # loader 运行后写入的状态文件
├── tests/                     # 单元测试
└── main.py                    # GUI 入口（转发到 mos_quant.ui.qt_app）
```

## 配置与缓存说明

- `config/`：你可以手动改（例如新增/替换 `probe_urls.json` 里的 URL）。
- `cache/`：运行期自动生成（缓存 + 状态），不建议提交到 git。

## 运行测试

```bash
python3 -m unittest -v
```

注：`tests/test_akshare_market_data.py` 会进行真实网络请求；网络不可用时可能失败。

## 相关文档

- [AKShare Document](https://akshare.akfamily.xyz/introduction.html)
- [AKTools Document](https://aktools.akfamily.xyz)
