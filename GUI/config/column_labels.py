from __future__ import annotations

COLUMN_LABELS = {
    "date": "日期",
    "datetime": "日期时间",
    "open": "开盘价",
    "high": "最高价",
    "low": "最低价",
    "close": "收盘价",
    "volume": "成交量",
    "amount": "成交额",
    "turnover": "换手率",
    "outstanding_share": "流通股本",
    "turnover_rate": "换手率",
    "amplitude": "振幅",
    "change_percent": "涨跌幅",
    "change_amount": "涨跌额",
    "volume_rate": "量比",
    "open_interest": "持仓量",
    "代码": "代码",
    "名称": "名称",
    "日期": "日期",
    "开盘": "开盘价",
    "收盘": "收盘价",
    "最高": "最高价",
    "最低": "最低价",
    "成交量": "成交量",
    "成交额": "成交额",
    "振幅": "振幅",
    "涨跌幅": "涨跌幅",
    "涨跌额": "涨跌额",
    "换手率": "换手率",
    "股票代码": "股票代码",
}


def to_chinese_headers(columns: list[str]) -> list[str]:
    localized: list[str] = []
    for col in columns:
        raw = str(col).strip()
        key = raw.lower()
        localized.append(COLUMN_LABELS.get(raw, COLUMN_LABELS.get(key, raw)))
    return localized
