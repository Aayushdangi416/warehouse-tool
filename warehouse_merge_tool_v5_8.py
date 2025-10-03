# warehouse_merge_tool_v5_8.py
# -*- coding: utf-8 -*-
"""
Warehouse Excel Merge Tool (v5.8)

更新点（相对 v5.7）：
- 产品信息表结构改为：A=SKU，B=Weight(kg)，C/D/E=Length/Width/Height（单位：厘米）
- 自动计算 UnitVolM3 = (C * D * E) / 1_000_000（cm³ -> m³）
- Single Item 导出仍包含列：Location, SKU, Qty
"""

import os, re
from datetime import datetime
import tkinter as tk
from tkinter import filedialog, messagebox, ttk
import tkinter.font as tkfont
import pandas as pd

APP_TITLE = "Warehouse Excel Merge Tool (v5.8)"

# -------------------- Utils --------------------
def clean_str_series(s: pd.Series) -> pd.Series:
    s = s.astype(str).str.strip()
    s = s.replace({"": pd.NA, "nan": pd.NA, "NaN": pd.NA, "None": pd.NA, "NULL": pd.NA})
    return s

def force_numeric(s: pd.Series, default=0.0) -> pd.Series:
    v = pd.to_numeric(s, errors="coerce")
    return v.fillna(default) if default is not None else v

def extract_aisle(location: str) -> int:
    if not isinstance(location, str): return 10_000
    parts = str(location).split("-")
    if len(parts) >= 2:
        m = re.search(r"\d+", parts[1])
        if m:
            try: return int(m.group(0))
            except: return 10_000
    return 10_000

def _usecols_for_main(sku_col_letter: str) -> str:
    # 读取：B=Location, <SKU列>=SKU, H(原数量), R(扣减)
    sku_col_letter = (sku_col_letter or "F").strip().upper()
    return f"B,{sku_col_letter},H,R"

# -------------------- Loaders (RAW & Aggregated) --------------------
def read_main_excel_raw(path: str, sku_col_letter: str) -> pd.DataFrame:
    """读取原始主数据（不聚合），数量按 Qty = H - R。"""
    try:
        df = pd.read_excel(path, usecols=_usecols_for_main(sku_col_letter), engine="openpyxl")
    except Exception:
        return pd.DataFrame(columns=["Location","SKU","Qty"])
    if df.empty:
        return pd.DataFrame(columns=["Location","SKU","Qty"])

    df.columns = ["Location", "SKU", "H", "R"]
    df["Location"] = clean_str_series(df["Location"])
    df["SKU"]      = clean_str_series(df["SKU"]).str.upper()

    df["Qty"] = force_numeric(df["H"], default=0.0) - force_numeric(df["R"], default=0.0)
    df = df.drop(columns=["H","R"])
    df = df.dropna(subset=["Location","SKU"])
    return df

def read_main_excel(path: str, sku_col_letter: str) -> pd.DataFrame:
    """读取主数据（聚合版），数量按 Qty = H - R，然后按 (Location, SKU) 聚合。"""
    try:
        df = pd.read_excel(path, usecols=_usecols_for_main(sku_col_letter), engine="openpyxl")
    except Exception:
        return pd.DataFrame(columns=["Location","SKU","Qty"])
    if df.empty:
        return pd.DataFrame(columns=["Location","SKU","Qty"])

    df.columns = ["Location", "SKU", "H", "R"]
    df["Location"] = clean_str_series(df["Location"])
    df["SKU"]      = clean_str_series(df["SKU"]).str.upper()
    df["Qty"]      = force_numeric(df["H"], default=0.0) - force_numeric(df["R"], default=0.0)
    df = df.drop(columns=["H","R"])

    df = df.dropna(subset=["Location","SKU"])
    if df.empty: return pd.DataFrame(columns=["Location","SKU","Qty"])
    df = df.groupby(["Location","SKU"], dropna=False, as_index=False)["Qty"].sum()
    return df

def read_product_excel(path: str) -> pd.DataFrame:
    """
    产品信息表：
      A = SKU
      B = Weight(kg)
      C = Length(cm)
      D = Width(cm)
      E = Height(cm)
    自动计算 UnitVolM3 = (C*D*E)/1_000_000
    """
    try:
        prod = pd.read_excel(path, usecols="A:E", engine="openpyxl")
    except Exception:
        # 返回带目标列名的空表
        return pd.DataFrame(columns=["SKU","WeightKg","LenCM","WidCM","HeiCM","UnitVolM3"])
    if prod.empty:
        return pd.DataFrame(columns=["SKU","WeightKg","LenCM","WidCM","HeiCM","UnitVolM3"])

    prod.columns = ["SKU","WeightKg","LenCM","WidCM","HeiCM"]
    prod["SKU"] = clean_str_series(prod["SKU"]).str.upper()

    # 数值化
    for col in ["WeightKg","LenCM","WidCM","HeiCM"]:
        prod[col] = pd.to_numeric(prod[col], errors="coerce")

    # 计算体积（m³）
    prod["UnitVolM3"] = (prod["LenCM"] * prod["WidCM"] * prod["HeiCM"]) / 1_000_000.0

    # 去空 SKU，保留最新
    prod = prod.dropna(subset=["SKU"]).drop_duplicates(subset=["SKU"], keep="last")
    return prod

# -------------------- Weight Filters (< / ≤) --------------------
def apply_weight_filter(df_all: pd.DataFrame, prod_map: pd.DataFrame, weight_thr: float, keep_missing: bool, inclusive: bool=False) -> pd.DataFrame:
    merged = df_all.merge(prod_map[["SKU","WeightKg"]], on="SKU", how="left")
    merged["_has_weight"] = pd.to_numeric(merged["WeightKg"], errors="coerce").notna()
    if not keep_missing:
        merged = merged[merged["_has_weight"]]
    w = pd.to_numeric(merged["WeightKg"], errors="coerce")
    if inclusive:
        keep_mask = (w <= float(weight_thr)) | (~merged["_has_weight"] & keep_missing)
    else:
        keep_mask = (w <  float(weight_thr)) | (~merged["_has_weight"] & keep_missing)
    merged = merged[keep_mask]
    return merged.drop(columns=["WeightKg","_has_weight"])

def apply_weight_filter_raw(df_raw: pd.DataFrame, prod_map: pd.DataFrame, weight_thr: float, keep_missing: bool, inclusive: bool=False) -> pd.DataFrame:
    merged = df_raw.merge(prod_map[["SKU","WeightKg"]], on="SKU", how="left")
    merged["_has_weight"] = pd.to_numeric(merged["WeightKg"], errors="coerce").notna()
    if not keep_missing:
        merged = merged[merged["_has_weight"]]
    w = pd.to_numeric(merged["WeightKg"], errors="coerce")
    if inclusive:
        keep_mask = (w <= float(weight_thr)) | (~merged["_has_weight"] & keep_missing)
    else:
        keep_mask = (w <  float(weight_thr)) | (~merged["_has_weight"] & keep_missing)
    merged = merged[keep_mask]
    return merged.drop(columns=["WeightKg","_has_weight"])

# -------------------- Core Logic --------------------
def choose_target_location(rows_except_min: pd.DataFrame, min_aisle: int, aisle_range: int) -> str:
    if rows_except_min.empty: return ""
    rows = rows_except_min.copy()
    rows["Aisle"] = rows["Location"].apply(extract_aisle)
    rows["dist"]  = (rows["Aisle"] - min_aisle).abs()
    within = rows[rows["dist"] <= aisle_range]
    if not within.empty:
        cand = within.sort_values(["Qty","dist"], ascending=[False,True]).iloc[0]
        return str(cand["Location"])
    cand = rows.sort_values(["dist","Qty"], ascending=[True,False]).iloc[0]
    return str(cand["Location"])

def build_sku_merging(df_all: pd.DataFrame, diff_thr: float, from_qty_max: float, aisle_range: int, include_rc: bool) -> pd.DataFrame:
    base = df_all.copy()
    if not include_rc:
        base = base.loc[~base["Location"].str.contains("RC", case=False, na=False)]
    cols = ["SKU","From_Location","To_Location","Transfer_Qty"]
    if base.empty: return pd.DataFrame(columns=cols)
    out = []
    for sku, g in base.groupby("SKU", dropna=False):
        if g.shape[0] <= 1: continue
        qmax, qmin = g["Qty"].max(), g["Qty"].min()
        if (qmax - qmin) <= float(diff_thr): continue
        min_row = g.sort_values(["Qty","Location"], ascending=[True,True]).iloc[0]
        from_loc = str(min_row["Location"]); from_qty = float(min_row["Qty"])
        if not (from_qty < float(from_qty_max)): continue
        to_loc = choose_target_location(g[g["Location"]!=from_loc][["Location","Qty"]], extract_aisle(from_loc), int(aisle_range))
        out.append({"SKU": sku, "From_Location": from_loc, "To_Location": to_loc, "Transfer_Qty": from_qty})
    out = pd.DataFrame(out, columns=cols)
    if not out.empty: out = out.sort_values(["SKU","From_Location"]).reset_index(drop=True)
    return out

def build_single_item_with_volume(
    df_all: pd.DataFrame,
    prod_map: pd.DataFrame,
    unique_qty_max: float,
    include_rc: bool = False,
    per_sku_vol_max: float = 0.5,   # 单 SKU@Location 总体积 < 0.5 m³
    per_loc_vol_max: float = 1.0    # Location 全部 SKU 总体积 < 1.0 m³
) -> pd.DataFrame:
    """
    Single Item：
      1) 按 (Location, SKU) 聚合数量（同库位多条合并）；
      2) 只保留“SKU 仅出现在 1 个库位”的情况；
      3) 限制：行体积(RowVol) < per_sku_vol_max，且库位总体积(LocTotalVol) < per_loc_vol_max；
      4) 限制：Qty ≤ unique_qty_max。
    输出: Location, SKU, Qty
    """
    base = df_all.copy()
    if not include_rc:
        base = base.loc[~base["Location"].str.contains("RC", case=False, na=False)]
    if base.empty:
        return pd.DataFrame(columns=["Location","SKU","Qty"])

    # 1) 聚合 (Location, SKU)
    agg = base.groupby(["Location", "SKU"], as_index=False, dropna=False)["Qty"].sum()

    # 2) SKU 仅在一个库位出现
    sku_loc_counts = agg.groupby("SKU")["Location"].nunique()
    single_loc_skus = set(sku_loc_counts[sku_loc_counts == 1].index)
    agg = agg[agg["SKU"].isin(single_loc_skus)]
    if agg.empty:
        return pd.DataFrame(columns=["Location","SKU","Qty"])

    # 3) 体积限制
    prod_needed = prod_map[["SKU", "UnitVolM3"]].copy()
    prod_needed["UnitVolM3"] = pd.to_numeric(prod_needed["UnitVolM3"], errors="coerce")
    joined = agg.merge(prod_needed, on="SKU", how="left")
    joined = joined[joined["UnitVolM3"].notna()]
    if joined.empty:
        return pd.DataFrame(columns=["Location","SKU","Qty"])

    joined["RowVol"] = joined["Qty"].astype(float) * joined["UnitVolM3"].astype(float)
    loc_vol = joined.groupby("Location", as_index=False)["RowVol"].sum().rename(columns={"RowVol": "LocTotalVol"})
    joined = joined.merge(loc_vol, on="Location", how="left")

    vol_ok = (joined["RowVol"] < float(per_sku_vol_max)) & (joined["LocTotalVol"] < float(per_loc_vol_max))
    joined = joined[vol_ok]
    if joined.empty:
        return pd.DataFrame(columns=["Location","SKU","Qty"])

    # 4) 数量阈值
    res = joined.loc[joined["Qty"] <= float(unique_qty_max), ["Location", "SKU", "Qty"]].copy()

    if not res.empty:
        res["Aisle"] = res["Location"].apply(extract_aisle)
        res = res.sort_values(["Aisle", "Location"]).drop(columns=["Aisle"]).reset_index(drop=True)

    return res

# -------------------- Export Helpers --------------------
def export_with_info(path: str, sheets: dict, meta: dict):
    with pd.ExcelWriter(path, engine="openpyxl") as w:
        for name, df in sheets.items():
            df.to_excel(w, index=False, sheet_name=name[:31] or "Sheet")
        info = pd.DataFrame({"Field": list(meta.keys()), "Value": list(meta.values())})
        info.to_excel(w, index=False, sheet_name="Info")

# -------------------- UI --------------------
class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title(APP_TITLE)
        try:
            import ctypes; ctypes.windll.shcore.SetProcessDpiAwareness(1)
        except Exception: pass
        base_size=12
        for name in ("TkDefaultFont","TkTextFont","TkMenuFont","TkHeadingFont","TkTooltipFont","TkFixedFont"):
            try: tkfont.nametofont(name).configure(size=base_size)
            except: pass
        ttk.Style(self).configure(".", font=("Segoe UI", base_size))
        self.geometry("1140x820"); self.minsize(920,660); self.resizable(True,True)
        self._is_full=False; self.bind("<F11>", lambda e:self.toggle_full()); self.bind("<Escape>", lambda e:self.restore())

        top=ttk.Frame(self,padding=(8,6)); top.pack(side="top",fill="x")
        ttk.Button(top,text="Maximize",command=self.maximize).pack(side="left",padx=4)
        ttk.Button(top,text="Toggle Fullscreen (F11)",command=self.toggle_full).pack(side="left",padx=4)
        ttk.Button(top,text="Restore (Esc)",command=self.restore).pack(side="left",padx=4)

        wrap=ttk.Frame(self); wrap.pack(fill="both",expand=True)
        # paths & params
        self.src=tk.StringVar(); self.prod=tk.StringVar(); self.out=tk.StringVar()
        self.v_diff=tk.StringVar(value="10"); self.v_from=tk.StringVar(value="10")
        self.v_aisle=tk.StringVar(value="5"); self.v_unique=tk.StringVar(value="2")
        self.v_weight=tk.StringVar(value="20")
        self.keep_missing = tk.BooleanVar(value=False)
        self.include_rc    = tk.BooleanVar(value=False)
        self.weight_inclusive = tk.BooleanVar(value=False)  # Weight uses ≤
        self.sku_col_letter = tk.StringVar(value="F")       # main file's SKU column

        for r in range(24): wrap.grid_rowconfigure(r,weight=0)
        for c in range(4):  wrap.grid_columnconfigure(c,weight=1)
        pad={"padx":14,"pady":10}

        ttk.Label(wrap,text="Source Excel (B=Location, [SKU col]=SKU, H=Qty, R=Deduct):").grid(row=0,column=0,sticky="w",**pad)
        ttk.Entry(wrap,textvariable=self.src).grid(row=1,column=0,columnspan=3,sticky="we",**pad)
        ttk.Button(wrap,text="Browse...",command=self.pick_src).grid(row=1,column=3,sticky="we",**pad)

        ttk.Label(wrap,text="SKU Column Letter (Main File):").grid(row=2,column=0,sticky="e",**pad)
        ttk.Entry(wrap,width=8,textvariable=self.sku_col_letter).grid(row=2,column=1,sticky="w",**pad)

        ttk.Label(wrap,text="Product Info Excel (A=SKU, B=Weight[kg], C/D/E=Len/Wid/Hei[cm]):").grid(row=3,column=0,sticky="w",**pad)
        ttk.Entry(wrap,textvariable=self.prod).grid(row=4,column=0,columnspan=3,sticky="we",**pad)
        ttk.Button(wrap,text="Browse...",command=self.pick_prod).grid(row=4,column=3,sticky="we",**pad)

        ttk.Label(wrap,text="Output Folder:").grid(row=5,column=0,sticky="w",**pad)
        ttk.Entry(wrap,textvariable=self.out).grid(row=6,column=0,columnspan=3,sticky="we",**pad)
        ttk.Button(wrap,text="Select...",command=self.pick_out).grid(row=6,column=3,sticky="we",**pad)

        box=ttk.LabelFrame(wrap,text="Parameters")
        box.grid(row=7,column=0,columnspan=4,sticky="we",padx=14,pady=10)
        for c in range(12): box.grid_columnconfigure(c,weight=1)
        ttk.Label(box,text="Difference Threshold (>):").grid(row=0,column=0,sticky="e",padx=8,pady=8)
        ttk.Entry(box,width=12,textvariable=self.v_diff).grid(row=0,column=1,sticky="w",padx=8,pady=8)
        ttk.Label(box,text="From Qty <").grid(row=0,column=2,sticky="e",padx=8,pady=8)
        ttk.Entry(box,width=12,textvariable=self.v_from).grid(row=0,column=3,sticky="w",padx=8,pady=8)
        ttk.Label(box,text="Aisle Range ≤").grid(row=0,column=4,sticky="e",padx=8,pady=8)
        ttk.Entry(box,width=12,textvariable=self.v_aisle).grid(row=0,column=5,sticky="w",padx=8,pady=8)
        ttk.Label(box,text="Single Item Qty ≤").grid(row=0,column=6,sticky="e",padx=8,pady=8)
        ttk.Entry(box,width=12,textvariable=self.v_unique).grid(row=0,column=7,sticky="w",padx=8,pady=8)
        ttk.Label(box,text="Weight < (kg)").grid(row=0,column=8,sticky="e",padx=8,pady=8)
        ttk.Entry(box,width=12,textvariable=self.v_weight).grid(row=0,column=9,sticky="w",padx=8,pady=8)

        ttk.Checkbutton(box,text="Include RC locations",variable=self.include_rc).grid(row=1,column=0,columnspan=2,sticky="w",padx=8)
        ttk.Checkbutton(box,text="Keep missing weights",variable=self.keep_missing).grid(row=1,column=2,columnspan=2,sticky="w",padx=8)
        ttk.Checkbutton(box,text="Weight uses ≤ (include equality)",variable=self.weight_inclusive).grid(row=1,column=4,columnspan=3,sticky="w",padx=8)

        ttk.Button(wrap,text="Run – SKU Merging",command=self.run_sku).grid(row=8,column=0,columnspan=4,sticky="we",**pad)
        ttk.Button(wrap,text="Run – Single Item Merging",command=self.run_single).grid(row=9,column=0,columnspan=4,sticky="we",**pad)
        ttk.Button(wrap,text="Run – Diagnostics (export all stages)",command=self.run_diag).grid(row=10,column=0,columnspan=4,sticky="we",**pad)
        ttk.Button(wrap,text="Export Bad Locations Detail",command=self.export_bad_locs).grid(row=11,column=0,columnspan=4,sticky="we",**pad)

        tips = (
            "- Qty = H - R（两列强制数值）。主文件可选哪一列作为 SKU（默认 F）。\n"
            "- 产品表：A=SKU, B=Weight(kg), C/D/E=长/宽/高(厘米)；系统自动换算体积到 m³。\n"
            "- Single Item：SKU 全表唯一 + Qty≤阈值 + 体积限制（SKU@Loc<0.5m³ 且 Loc<1.0m³）。"
        )
        ttk.Label(wrap,text=tips,foreground="#555",justify="left").grid(row=12,column=0,columnspan=4,sticky="we",padx=14,pady=8)

    # window helpers / pickers / parsers (same as v5.7) ...
    def maximize(self): 
        try: self.state('zoomed')
        except: self.toggle_full()
    def toggle_full(self): 
        self._is_full=not self._is_full; self.attributes("-fullscreen",self._is_full)
    def restore(self): 
        self.attributes("-fullscreen",False); self._is_full=False; 
        try: self.state('normal')
        except: pass
    def pick_src(self): 
        p=filedialog.askopenfilename(title="Select Source Excel",filetypes=[("Excel","*.xlsx *.xls")])
        if p: self.src.set(p)
    def pick_prod(self): 
        p=filedialog.askopenfilename(title="Select Product Info Excel",filetypes=[("Excel","*.xlsx *.xls")])
        if p: self.prod.set(p)
    def pick_out(self): 
        p=filedialog.askdirectory(title="Select Output Folder")
        if p: self.out.set(p)
    def _f(self, var, name):
        try: return float(var.get())
        except: raise ValueError(f"Invalid number for '{name}': {var.get()}")
    def _i(self, var, name):
        try: return int(float(var.get()))
        except: raise ValueError(f"Invalid integer for '{name}': {var.get()}")
    def _check_paths(self):
        if not self.src.get():  messagebox.showwarning(APP_TITLE,"Please select the source Excel file."); return False
        if not self.prod.get(): messagebox.showwarning(APP_TITLE,"Please select the product info Excel file."); return False
        if not self.out.get():  messagebox.showwarning(APP_TITLE,"Please select the output folder."); return False
        return True
    def _tag(self):
        return f"d{self.v_diff.get()}_f{self.v_from.get()}_a{self.v_aisle.get()}_u{self.v_unique.get()}_w{self.v_weight.get()}{'le' if self.weight_inclusive.get() else 'lt'}_km{int(self.keep_missing.get())}_rc{int(self.include_rc.get())}_sku{self.sku_col_letter.get().upper()}"

    # actions
    def run_sku(self):
        try:
            if not self._check_paths(): return
            diff=self._f(self.v_diff,"Difference Threshold")
            frm =self._f(self.v_from,"From Qty <")
            rng =self._i(self.v_aisle,"Aisle Range ≤")
            sku_col = self.sku_col_letter.get()

            prod = read_product_excel(self.prod.get())
            if prod.empty:
                messagebox.showwarning(APP_TITLE,"Product Info Excel is empty/invalid (A:E)."); return

            df_all = read_main_excel(self.src.get(), sku_col)
            if df_all.empty:
                messagebox.showwarning(APP_TITLE,"Source Excel is empty/invalid (B/SKU/H/R)."); return

            df_w = apply_weight_filter(df_all, prod, self._f(self.v_weight,"Weight < (kg)"),
                                       self.keep_missing.get(), inclusive=self.weight_inclusive.get())
            if not self.include_rc.get():
                df_w = df_w.loc[~df_w["Location"].str.contains("RC", case=False, na=False)]
            if df_w.empty:
                messagebox.showinfo(APP_TITLE,"No rows after Weight/RC filter."); return

            res = build_sku_merging(df_w, diff, frm, rng, include_rc=True)
            if res.empty:
                messagebox.showinfo(APP_TITLE,"No suggestions under current parameters."); return

            ts=datetime.now().strftime("%Y%m%d_%H%M%S")
            out=os.path.join(self.out.get(), f"SKU_Merging_{self._tag()}_{ts}.xlsx")
            export_with_info(out, {"SKU Merging":res}, {
                "Source File":os.path.basename(self.src.get()),
                "Product File":os.path.basename(self.prod.get()),
                "Generated At":datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "Params": f"diff>{diff}, from<{frm}, aisle≤{rng}, weight<{self.v_weight.get()} ({'≤' if self.weight_inclusive.get() else '<'}), keep_missing={self.keep_missing.get()}, include_rc={self.include_rc.get()}, sku_col={sku_col}, qty=H-R, vol=cm→m³(C*D*E/1e6)"
            })
            messagebox.showinfo(APP_TITLE, f"Done (SKU Merging)!\n\nSaved:\n{out}")
        except Exception as e:
            messagebox.showerror(APP_TITLE, f"Error (SKU Merging): {e}")

    def run_single(self):
        try:
            if not self._check_paths(): return
            uniq=self._f(self.v_unique,"Single Item Qty ≤")
            sku_col = self.sku_col_letter.get()

            prod = read_product_excel(self.prod.get())
            if prod.empty:
                messagebox.showwarning(APP_TITLE,"Product Info Excel is empty/invalid (A:E)."); return

            agg = read_main_excel(self.src.get(), sku_col)
            df_w = apply_weight_filter(agg, prod, self._f(self.v_weight,"Weight < (kg)"),
                                       self.keep_missing.get(), inclusive=self.weight_inclusive.get())
            if not self.include_rc.get():
                df_w = df_w.loc[~df_w["Location"].str.contains("RC", case=False, na=False)]
            if df_w.empty:
                messagebox.showinfo(APP_TITLE,"No rows after Weight/RC filter."); return

            # Single Item with volume constraints
            res = build_single_item_with_volume(
                df_w, prod, uniq,
                include_rc=True,           # RC 已在上方按 UI 处理
                per_sku_vol_max=0.5,       # <0.5 m³
                per_loc_vol_max=1.0        # <1.0 m³
            )
            if res.empty:
                messagebox.showinfo(APP_TITLE,"No items meet Single Item rules with volume constraints."); return

            ts=datetime.now().strftime("%Y%m%d_%H%M%S")
            out=os.path.join(self.out.get(), f"Single_Item_Merging_{self._tag()}_{ts}.xlsx")
            export_with_info(out, {"Single Item Merging":res}, {
                "Source File":os.path.basename(self.src.get()),
                "Product File":os.path.basename(self.prod.get()),
                "Generated At":datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "Params": f"unique≤{uniq}, volume: sku@loc<0.5m³ & loc<1.0m³ (cm→m³), weight<{self.v_weight.get()} ({'≤' if self.weight_inclusive.get() else '<'}), keep_missing={self.keep_missing.get()}, include_rc={self.include_rc.get()}, sku_col={sku_col}, qty=H-R"
            })
            messagebox.showinfo(APP_TITLE, f"Done (Single Item Merging)!\n\nSaved:\n{out}")
        except Exception as e:
            messagebox.showerror(APP_TITLE, f"Error (Single Item Merging): {e}")

    def run_diag(self):
        try:
            if not self._check_paths(): return
            diff=self._f(self.v_diff,"Difference Threshold")
            frm =self._f(self.v_from,"From Qty <")
            rng =self._i(self.v_aisle,"Aisle Range ≤")
            uniq=self._f(self.v_unique,"Single Item Qty ≤")
            sku_col = self.sku_col_letter.get()

            prod  = read_product_excel(self.prod.get())
            agg   = read_main_excel(self.src.get(), sku_col)
            agg_w = apply_weight_filter(agg, prod, self._f(self.v_weight,"Weight < (kg)"),
                                        self.keep_missing.get(), inclusive=self.weight_inclusive.get())
            if not self.include_rc.get():
                agg_w = agg_w.loc[~agg_w["Location"].str.contains("RC", case=False, na=False)]

            single_vol = build_single_item_with_volume(
                agg_w, prod, uniq, include_rc=True,
                per_sku_vol_max=0.5, per_loc_vol_max=1.0
            )
            sku_res = build_sku_merging(agg_w, diff, frm, rng, include_rc=True)

            stats = pd.DataFrame({
                "Stage":[
                    "Aggregated Main",
                    "After Weight/RC (Agg)",
                    "SKU Merging result",
                    "Single Item (with volume) result"
                ],
                "Rows":[len(agg), len(agg_w), len(sku_res), len(single_vol)]
            })

            ts=datetime.now().strftime("%Y%m%d_%H%M%S")
            out=os.path.join(self.out.get(), f"Diagnostics_{self._tag()}_{ts}.xlsx")
            export_with_info(out, {
                "Aggregated Main": agg,
                "Agg after Weight+RC": agg_w,
                "SKU Merging Result": sku_res,
                "Single Item (with volume) Result": single_vol,
                "Stats": stats
            }, {
                "Source File":os.path.basename(self.src.get()),
                "Product File":os.path.basename(self.prod.get()),
                "Generated At":datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "Params": f"diff>{diff}, from<{frm}, aisle≤{rng}, unique≤{uniq}, volume: cm→m³(C*D*E/1e6), weight<{self.v_weight.get()} ({'≤' if self.weight_inclusive.get() else '<'}), keep_missing={self.keep_missing.get()}, include_rc={self.include_rc.get()}, sku_col={sku_col}, qty=H-R"
            })
            messagebox.showinfo(APP_TITLE, f"Diagnostics exported.\n\nSaved:\n{out}")
        except Exception as e:
            messagebox.showerror(APP_TITLE, f"Error (Diagnostics): {e}")

    def export_bad_locs(self):
        """保留的调试工具：导出每个库位下的所有 SKU 明细（与 Single Item 逻辑无关，仅便于人工核对）。"""
        try:
            if not self._check_paths(): return
            sku_col = self.sku_col_letter.get()
            raw = read_main_excel_raw(self.src.get(), sku_col)
            if raw.empty:
                messagebox.showwarning(APP_TITLE,"Source Excel is empty or invalid (B/SKU/H/R)."); return
            if not self.include_rc.get():
                raw = raw.loc[~raw["Location"].str.contains("RC", case=False, na=False)]
            detail = raw.sort_values(["Location","SKU"])
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            out = os.path.join(self.out.get(), f"All_Locations_Detail_rc{int(self.include_rc.get())}_sku{sku_col.upper()}_{ts}.xlsx")
            with pd.ExcelWriter(out, engine="openpyxl") as w:
                detail.to_excel(w, index=False, sheet_name="Locations_Detail")
                info = pd.DataFrame({
                    "Field": ["Source File", "Generated At", "Note"],
                    "Value": [
                        os.path.basename(self.src.get()),
                        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        "Qty computed as H - R; this is a raw detail export for manual review."
                    ]
                })
                info.to_excel(w, index=False, sheet_name="Info")
            messagebox.showinfo(APP_TITLE, f"Exported.\n\nSaved:\n{out}")
        except Exception as e:
            messagebox.showerror(APP_TITLE, f"Error (Export Locations Detail): {e}")

if __name__ == "__main__":
    try:
        import ctypes; ctypes.windll.shcore.SetProcessDpiAwareness(1)
    except Exception: pass
    app=App(); app.mainloop()
