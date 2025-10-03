from flask import Flask, request, render_template_string, send_file
import pandas as pd
import os
from datetime import datetime

# Import your existing functions from warehouse_merge_tool
from warehouse_merge_tool_v5_8 import read_main_excel, read_product_excel, build_sku_merging, apply_weight_filter

app = Flask(__name__)

# Home page with file upload
@app.route("/", methods=["GET", "POST"])
def home():
    if request.method == "POST":
        # Save uploaded files
        main_file = request.files["main"]
        product_file = request.files["product"]

        main_path = "main.xlsx"
        prod_path = "product.xlsx"
        main_file.save(main_path)
        product_file.save(prod_path)

        # Run logic
        prod = read_product_excel(prod_path)
        df_all = read_main_excel(main_path, "F")  # default SKU column "F"
        df_w = apply_weight_filter(df_all, prod, weight_thr=20, keep_missing=True)

        result = build_sku_merging(df_w, diff_thr=10, from_qty_max=10, aisle_range=5, include_rc=True)

        # Save result
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        out_path = f"result_{ts}.xlsx"
        result.to_excel(out_path, index=False)

        return send_file(out_path, as_attachment=True)

    # Simple HTML form
    return render_template_string("""
    <h2>🚀 Warehouse Merge Tool (Web)</h2>
    <form method="post" enctype="multipart/form-data">
      <p>Main Excel File: <input type="file" name="main"></p>
      <p>Product Excel File: <input type="file" name="product"></p>
      <p><button type="submit">Run Merge</button></p>
    </form>
    """)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
