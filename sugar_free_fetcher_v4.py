# -*- coding: utf-8 -*-
"""
白糖基本面数据采集 —— 完整版 v4
新增：泛糖网原糖/现货价格、进口成本、进口利润空间、WTI原油、龙虎榜数据清洗
运行后自动生成 sugar_data.json，配合 sugar_dashboard.html 使用

安装依赖：
  pip install gm akshare pandas openpyxl requests

使用：
  掘金终端保持登录 → 密钥管理 → 复制Token → 填入MY_TOKEN
"""

import warnings
warnings.filterwarnings("ignore")

from gm.api import set_token, history, get_instruments
SEC_TYPE_FUTURES = 1
import akshare as ak
import pandas as pd
import requests
import io, os, json, re
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

# ============================================================
# 配置区 —— 只需修改这里
# ============================================================
MY_TOKEN   = "068a8bdfc09f95d19a9aabf266bbce139c5408de"   # 掘金Token
SPOT_PRICE = 5460                 # 广西现货价（自动获取失败时的备用值）
DAYS_BACK  = 365
SUGAR_MAIN = "CZCE.SR"
SUGAR_SPOT = "CZCE.SR605"        # 当前主力合约

set_token(MY_TOKEN)
os.environ["NO_PROXY"] = "*"
os.environ["no_proxy"] = "*"

# ────────────────────────────────────────────────────────────
# 工具函数
# ────────────────────────────────────────────────────────────
def date_range(days_back=DAYS_BACK):
    end   = datetime.today().strftime("%Y-%m-%d")
    start = (datetime.today() - timedelta(days=days_back)).strftime("%Y-%m-%d")
    return start, end

def section(title):
    print(f"\n{'─'*55}\n  {title}\n{'─'*55}")

def last_trade_day(offset=1):
    d = datetime.today() - timedelta(days=offset)
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return d.strftime("%Y%m%d")


# ============================================================
# ① 掘金：日线行情（含open/high/low/volume）
# ============================================================
def fetch_price(symbol=SUGAR_MAIN, days_back=DAYS_BACK):
    section(f"行情数据  {symbol}")
    start, end = date_range(days_back)
    df = history(symbol=symbol, frequency="1d", start_time=start, end_time=end,
                 fields="open,high,low,close,volume,amount,position", df=True)
    date_col = next((c for c in ["eob","bob","date","created_at","time"] if c in df.columns), None)
    if date_col:
        df = df.rename(columns={date_col: "trade_date"})
        df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date
    else:
        df.insert(0, "trade_date", range(1, len(df)+1))
    print(f"获取 {len(df)} 条日线")
    show_cols = [c for c in ["trade_date","open","high","low","close","volume","position"] if c in df.columns]
    print(df.tail(5)[show_cols])
    return df


# ============================================================
# ② 掘金：合约信息（只显示活跃合约）
# ============================================================
def fetch_contracts():
    section("活跃合约信息")
    try:
        instr = get_instruments(exchanges="CZCE", sec_type1=SEC_TYPE_FUTURES, df=True)
    except TypeError:
        instr = get_instruments(exchanges="CZCE", df=True)
    sugar = instr[instr["symbol"].str.match(r"CZCE\.SR\d{3}$")].copy()
    cols  = [c for c in ["symbol","sec_name","listed_date","delisted_date","multiplier"] if c in sugar.columns]
    if "delisted_date" in sugar.columns:
        sugar["_d"] = pd.to_datetime(sugar["delisted_date"], utc=True, errors="coerce")
        active = sugar[sugar["_d"] >= pd.Timestamp(datetime.today(), tz="UTC")][cols]
    else:
        active = sugar[cols]
    print(f"活跃合约 {len(active)} 个")
    return active


# ============================================================
# ③ 掘金：跨期价差
# ============================================================
def fetch_spread(front="CZCE.SR605", back="CZCE.SR609", days_back=120):
    section(f"跨期价差  {front} - {back}")
    start, end = date_range(days_back)
    def get_close(sym):
        df = history(symbol=sym, frequency="1d", start_time=start, end_time=end, fields="close", df=True)
        if df.empty or len(df.columns) == 0:
            return pd.DataFrame(columns=[sym])
        return df[[df.columns[-1]]].rename(columns={df.columns[-1]: sym}).reset_index(drop=True)
    df = pd.concat([get_close(front), get_close(back)], axis=1).dropna()
    df["spread"] = (df[front] - df[back]).round(1)
    df.index = range(1, len(df)+1)
    print(f"最新价差：{df['spread'].iloc[-1]} 元/吨")
    return df


# ============================================================
# ④ 基差计算
# ============================================================
def calc_basis(price_df, spot=SPOT_PRICE):
    section("基差计算")
    fut   = round(price_df["close"].iloc[-1], 1)
    basis = round(spot - fut, 1)
    rate  = round(basis / fut * 100, 2)
    print(f"期货：{fut}  现货：{spot}  基差：{basis}  基差率：{rate}%")
    return {"futures": fut, "spot": spot, "basis": basis, "basis_rate": rate}


# ============================================================
# ⑤ AKShare：注册仓单历史
# ============================================================
def fetch_warehouse_receipt():
    section("注册仓单历史")
    try:
        td  = last_trade_day(1)
        d45 = (datetime.today() - timedelta(days=45)).strftime("%Y%m%d")
        df  = ak.get_receipt(start_date=d45, end_date=td, vars_list=["SR"])
        if not df.empty:
            print(df.tail(10).to_string(index=False))
            return df
    except Exception as e:
        print(f"get_receipt失败：{e}")
    try:
        data = ak.futures_warehouse_receipt_czce()
        if isinstance(data, dict) and "SR" in data:
            df = data["SR"]
            total = df[df.iloc[:,0].astype(str).str.contains("总计")]["仓单数量"].values
            print(f"白糖仓单总量：{total[0] if len(total) else '未知'} 张")
            return df
    except Exception as e:
        print(f"明细接口失败：{e}")
    return pd.DataFrame()


# ============================================================
# ⑥ 泛糖网：原糖价格 + 现货价格（实时）
# ============================================================
def fetch_ny_sugar():
    section("原糖价格 + 现货价格（泛糖网）")
    try:
        df = ak.index_sugar_msweet()
        if not df.empty:
            df = df.rename(columns={"日期":"date","原糖价格":"close","现货价格":"spot","综合价格":"index"})
            df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
            print(f"获取糖价指数 {len(df)} 条，最新：{df.iloc[-1]['date']}")
            print(df[["date","index","close","spot"]].tail(5).to_string(index=False))
            return df
    except Exception as e:
        print(f"糖价指数失败：{e}")
    return pd.DataFrame()


# ============================================================
# ⑦ 泛糖网：进口成本与利润空间
# ============================================================
def fetch_import_cost():
    section("进口成本与利润空间（泛糖网）")
    try:
        df = ak.index_inner_quote_sugar_msweet()
        if not df.empty:
            df = df.rename(columns={"日期":"date"})
            df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
            print(f"获取进口报价 {len(df)} 条，最新：{df.iloc[-1]['date']}")
            cols = ["date","柳州现货价","广州现货价","巴西糖","泰国糖"]
            avail = [c for c in cols if c in df.columns]
            print(df[avail].tail(5).to_string(index=False))
            return df
    except Exception as e:
        print(f"进口报价失败：{e}")
    return pd.DataFrame()


# ============================================================
# ⑧ 泛糖网：进口利润空间
# ============================================================
def fetch_import_export():
    section("进口利润空间（泛糖网）")
    try:
        df = ak.index_outer_quote_sugar_msweet()
        if not df.empty:
            df = df.rename(columns={"日期":"date"})
            df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
            print(f"获取进口利润 {len(df)} 条，最新：{df.iloc[-1]['date']}")
            print(df.tail(5).to_string(index=False))
            return df
    except Exception as e:
        print(f"进口利润接口失败：{e}")
    return pd.DataFrame()


# ============================================================
# ⑨ WTI原油价格（用于巴西糖醇比预警）
# ============================================================
def fetch_crude_oil():
    section("WTI原油价格（用于糖醇比预警）")
    try:
        df = ak.futures_foreign_hist(symbol="CL")
        if not df.empty:
            df = df.tail(120).copy()
            df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
            last = df.iloc[-1]
            print(f"WTI原油最新：{last['date']} 收盘 {last['close']:.2f} 美元/桶")
            return df
    except Exception as e:
        print(f"WTI原油获取失败：{e}")
    return pd.DataFrame()


# ============================================================
# ⑩ 国内产销存（东方财富备用）
# ============================================================
def fetch_production_sales():
    section("国内产销存数据")
    try:
        df = ak.futures_inventory_em(symbol="SR")
        if not df.empty:
            print(f"获取SR库存 {len(df)} 条（东方财富）")
            return df
    except Exception as e:
        print(f"库存接口失败：{e}")
    return pd.DataFrame()


# ============================================================
# ⑪ 郑商所直连：解析单日龙虎榜（含合约名清洗）
# ============================================================
def parse_rank_xlsx(content, trade_date):
    import openpyxl
    df   = pd.read_excel(io.BytesIO(content), engine="openpyxl", header=0)
    col0 = df.columns[0]
    sr_rows = df[df[col0].astype(str).str.contains(r"合约：SR", na=False)].index.tolist()
    if not sr_rows:
        return pd.DataFrame()
    chunks = []
    for i, idx in enumerate(sr_rows):
        next_idx = sr_rows[i+1] if i+1 < len(sr_rows) else idx+25
        block = df.iloc[idx+2:next_idx].reset_index(drop=True)
        # 只保留名次列能转为数字的数据行
        block = block[pd.to_numeric(
            block.iloc[:,0].astype(str).str.replace(",",""), errors="coerce"
        ).notna()].copy()
        if block.empty:
            continue
        # 清洗合约名：只保留SR+3位数字
        raw_contract = str(df.iloc[idx][col0]).replace("合约：","").strip()
        match = re.search(r'SR\d{3}', raw_contract)
        contract = match.group(0) if match else raw_contract
        block.insert(0, "合约", contract)
        block.insert(0, "日期", trade_date)
        chunks.append(block)
    return pd.concat(chunks, ignore_index=True) if chunks else pd.DataFrame()

def fetch_rank_single(trade_date):
    year    = trade_date[:4]
    url     = f"http://www.czce.com.cn/cn/DFSStaticFiles/Future/{year}/{trade_date}/FutureDataHolding.xlsx"
    headers = {"User-Agent": "Mozilla/5.0", "Referer": "http://www.czce.com.cn"}
    proxies = {"http": None, "https": None}
    try:
        resp = requests.get(url, headers=headers, proxies=proxies, timeout=5)
        if resp.status_code == 200 and len(resp.content) > 1000:
            return parse_rank_xlsx(resp.content, trade_date)
    except Exception:
        pass
    return pd.DataFrame()


# ============================================================
# ⑫ 龙虎榜历史（并发+缓存，只保留新格式）
# ============================================================
def fetch_position_rank_history(days=30):
    section(f"龙虎榜历史（近{days}个交易日）")
    cache_file = "sugar_rank_cache.json"
    cache = {}
    if os.path.exists(cache_file):
        try:
            with open(cache_file, "r", encoding="utf-8") as f:
                raw = json.load(f)
            # 只保留含"合约"字段的新格式数据
            for td, records in raw.items():
                if records and isinstance(records, list) and "合约" in records[0]:
                    cache[td] = records
        except Exception:
            cache = {}

    trade_dates = []
    d = datetime.today() - timedelta(days=1)
    while len(trade_dates) < days:
        if d.weekday() < 5:
            trade_dates.append(d.strftime("%Y%m%d"))
        d -= timedelta(days=1)

    need_fetch = [td for td in trade_dates if td not in cache]
    print(f"缓存命中 {len(trade_dates)-len(need_fetch)} 天，需下载 {len(need_fetch)} 天")

    def fetch_and_cache(td):
        df = fetch_rank_single(td)
        return td, (df.to_dict(orient="records") if not df.empty else None)

    if need_fetch:
        with ThreadPoolExecutor(max_workers=8) as executor:
            for td, data in executor.map(fetch_and_cache, need_fetch):
                if data:
                    cache[td] = data
                    print(f"  ✓ {td}")
                else:
                    print(f"  - {td} 无数据")
        with open(cache_file, "w", encoding="utf-8") as f:
            json.dump(cache, f, ensure_ascii=False)

    all_data = [pd.DataFrame(cache[td]) for td in trade_dates if td in cache and cache[td]]
    if not all_data:
        print("龙虎榜历史获取失败")
        return pd.DataFrame(), pd.DataFrame()

    result = pd.concat(all_data, ignore_index=True)

    def to_num(s):
        return pd.to_numeric(s.astype(str).str.replace(",",""), errors="coerce")

    # 列结构：日期(0),合约(1),名次(2),成交会员(3),交易量(4),成交增减(5),
    #         买方会员(6),持买仓量(7),持买增减(8),卖方会员(9),持卖仓量(10),持卖增减(11)
    summary_rows = []
    for td, grp in result.groupby("日期"):
        summary_rows.append({
            "日期":   td,
            "多头持仓": int(to_num(grp.iloc[:,7]).sum() or 0),
            "多头增减": int(to_num(grp.iloc[:,8]).sum() or 0),
            "空头持仓": int(to_num(grp.iloc[:,10]).sum() or 0),
            "空头增减": int(to_num(grp.iloc[:,11]).sum() or 0),
            "净多头":  int((to_num(grp.iloc[:,7]).sum() or 0) - (to_num(grp.iloc[:,10]).sum() or 0)),
        })

    summary = pd.DataFrame(summary_rows).sort_values("日期")
    print(f"\n获取 {result['日期'].nunique()} 个交易日")
    print(summary.tail(5).to_string(index=False))
    return result, summary


# ============================================================
# ⑬ 龙虎榜当日（含列名清洗）
# ============================================================
def fetch_position_rank():
    section("龙虎榜（最新交易日）")
    td = last_trade_day(1)
    df = fetch_rank_single(td)
    if not df.empty:
        print(f"日期：{td}  共 {df['合约'].nunique()} 个合约")
        main = df["合约"].iloc[0]
        print(df[df["合约"] == main].head(10).to_string(index=False))
    else:
        print(f"无数据（{td}）")
    return df


# ============================================================
# ⑭ 导出 JSON（供 Dashboard 使用）
# ============================================================
def safe_list(df):
    if df is None or df.empty:
        return []
    try:
        return json.loads(df.to_json(orient="records", force_ascii=False, default_handler=str))
    except Exception:
        return []

def export_json(price_df, spread_df, receipt_df, basis_info,
                rank_today, rank_summary, ny_df, import_df,
                prod_df, import_cost_df=None, crude_df=None):
    section("生成 Dashboard 数据")

    # 郑糖价格（含日期、量价）
    price_list = []
    if price_df is not None and not price_df.empty:
        for _, r in price_df.iterrows():
            try:
                price_list.append({
                    "date":     str(r["trade_date"]) if "trade_date" in price_df.columns else "",
                    "open":     float(r["open"])     if pd.notna(r.get("open"))     else None,
                    "high":     float(r["high"])     if pd.notna(r.get("high"))     else None,
                    "low":      float(r["low"])      if pd.notna(r.get("low"))      else None,
                    "close":    float(r["close"]),
                    "volume":   int(r["volume"])     if pd.notna(r.get("volume"))   else 0,
                    "position": int(r["position"])   if pd.notna(r.get("position")) else 0,
                })
            except Exception:
                pass

    # 跨期价差（含索引）
    spread_list = []
    if spread_df is not None and not spread_df.empty:
        for i, (_, r) in enumerate(spread_df.iterrows()):
            if pd.notna(r["spread"]):
                spread_list.append({"idx": i+1, "spread": float(r["spread"])})

    # 仓单历史
    receipt_list = []
    if receipt_df is not None and not receipt_df.empty and "receipt" in receipt_df.columns:
        receipt_list = [
            {"date": str(r["date"]), "receipt": int(r["receipt"]), "receipt_chg": int(r["receipt_chg"])}
            for _, r in receipt_df.iterrows() if pd.notna(r["receipt"])
        ]

    # 龙虎榜汇总
    rank_summary_list = rank_summary.to_dict(orient="records") if rank_summary is not None and not rank_summary.empty else []

    # 龙虎榜当日（列名清洗）
    rank_today_list = []
    if rank_today is not None and not rank_today.empty:
        rt = rank_today.copy()
        cols = rt.columns.tolist()
        if len(cols) >= 12:
            col_map = {
                cols[0]: "日期", cols[1]: "合约",
                cols[2]: "名次",
                cols[3]: "成交会员", cols[4]: "交易量",  cols[5]: "成交增减",
                cols[6]: "多头会员", cols[7]: "持买仓量", cols[8]: "持买增减",
                cols[9]: "空头会员", cols[10]: "持卖仓量", cols[11]: "持卖增减",
            }
            rt = rt.rename(columns=col_map)
        rank_today_list = safe_list(rt.head(100))

    # 泛糖网原糖价格（含现货）
    ny_list = []
    if ny_df is not None and not ny_df.empty and "close" in ny_df.columns:
        for _, r in ny_df.tail(400).iterrows():
            try:
                row = {"date": str(r["date"]), "close": float(r["close"])}
                if "spot" in ny_df.columns and pd.notna(r.get("spot")):
                    row["spot"] = float(r["spot"])
                if "index" in ny_df.columns and pd.notna(r.get("index")):
                    row["index"] = float(r["index"])
                ny_list.append(row)
            except Exception:
                pass

    # 进口利润空间（泛糖网）
    import_list = []
    if import_df is not None and not import_df.empty:
        for _, r in import_df.tail(180).iterrows():
            try:
                import_list.append({
                    "date":            str(r["date"]),
                    "brazil_cost":     float(r["巴西糖进口成本"])     if pd.notna(r.get("巴西糖进口成本"))     else None,
                    "thailand_cost":   float(r["泰国糖进口成本"])     if pd.notna(r.get("泰国糖进口成本"))     else None,
                    "brazil_profit":   float(r["巴西糖进口利润空间"]) if pd.notna(r.get("巴西糖进口利润空间")) else None,
                    "thailand_profit": float(r["泰国糖进口利润空间"]) if pd.notna(r.get("泰国糖进口利润空间")) else None,
                })
            except Exception:
                pass

    # 进口成本（泛糖网 index_inner_quote）
    import_cost_list = []
    if import_cost_df is not None and not import_cost_df.empty:
        for _, r in import_cost_df.tail(180).iterrows():
            try:
                import_cost_list.append({
                    "date":      str(r["date"]),
                    "liuzhou":   float(r["柳州现货价"])  if pd.notna(r.get("柳州现货价"))  else None,
                    "guangzhou": float(r["广州现货价"])  if pd.notna(r.get("广州现货价"))  else None,
                    "brazil":    float(r["巴西糖"])      if pd.notna(r.get("巴西糖"))      else None,
                    "thailand":  float(r["泰国糖"])      if pd.notna(r.get("泰国糖"))      else None,
                })
            except Exception:
                pass

    # WTI原油
    crude_list = []
    if crude_df is not None and not crude_df.empty:
        for _, r in crude_df.iterrows():
            try:
                crude_list.append({
                    "date":  str(r["date"]),
                    "close": float(r["close"]),
                    "high":  float(r["high"]),
                    "low":   float(r["low"]),
                })
            except Exception:
                pass

    # 自动现货价（优先用泛糖网）
    auto_spot = None
    if ny_list and ny_list[-1].get("spot"):
        auto_spot = ny_list[-1]["spot"]

    data = {
        "update_time":   datetime.now().strftime("%Y-%m-%d %H:%M"),
        "basis":         basis_info.get("basis"),
        "spot_price":    auto_spot or basis_info.get("spot"),
        "futures_price": basis_info.get("futures"),
        "price":         price_list,
        "spread":        spread_list,
        "receipt":       receipt_list,
        "rank_summary":  rank_summary_list,
        "rank_today":    rank_today_list,
        "ny_sugar":      ny_list,
        "import_export": import_list,
        "import_cost":   import_cost_list,
        "crude_oil":     crude_list,
        "production":    safe_list(prod_df),
    }

    with open("sugar_data.json", "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print("✓ sugar_data.json 已生成")
    print("✓ 启动服务：python -m http.server 8888")
    print("✓ 浏览器访问：http://localhost:8888/sugar_dashboard.html")


# ============================================================
# 主流程
# ============================================================
def run():
    print("=" * 55)
    print("  白糖基本面数据采集（完整版 v4）")
    print(f"  运行时间：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 55)

    price_df    = fetch_price()
    fetch_contracts()
    spread_df   = fetch_spread(front="CZCE.SR605", back="CZCE.SR609")
    # 优先用泛糖网自动现货价，获取失败才用手动值
    _spot = SPOT_PRICE
    try:
        _ny_tmp = ak.index_sugar_msweet()
        if not _ny_tmp.empty and "现货价格" in _ny_tmp.columns:
            _s = float(_ny_tmp.iloc[-1]["现货价格"])
            if _s > 0:
                _spot = _s
                print(f"  现货价自动获取：{_spot} 元/吨（泛糖网）")
    except Exception:
        pass
    basis_info  = calc_basis(price_df, spot=_spot)
    receipt_df  = fetch_warehouse_receipt()
    rank_today  = fetch_position_rank()
    rank_hist, rank_summary = fetch_position_rank_history(days=30)
    ny_df       = fetch_ny_sugar()
    import_cost_df = fetch_import_cost()
    import_df   = fetch_import_export()
    crude_df    = fetch_crude_oil()
    prod_df     = fetch_production_sales()

    export_json(price_df, spread_df, receipt_df, basis_info,
                rank_today, rank_summary, ny_df, import_df,
                prod_df, import_cost_df, crude_df)

    section("汇总完成")
    for name, df in [
        ("郑糖日线", price_df), ("跨期价差", spread_df), ("注册仓单", receipt_df),
        ("龙虎榜",   rank_today), ("龙虎榜历史", rank_hist), ("多空汇总", rank_summary),
        ("纽约原糖", ny_df), ("进口利润", import_df), ("进口成本", import_cost_df),
        ("WTI原油",  crude_df), ("产销存",   prod_df),
    ]:
        status = f"{len(df)} 条" if df is not None and not df.empty else "无数据"
        print(f"  {name:<12} {status}")

if __name__ == "__main__":
    run()
