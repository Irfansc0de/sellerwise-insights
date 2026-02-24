from flask import Flask, render_template, request, redirect, session
import pandas as pd
import sqlite3
import os

app = Flask(__name__)
app.secret_key = "nxtsecurekey"

UPLOAD_FOLDER = "uploads"
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

# ---------------- DATABASE INIT ----------------

def init_db():
    conn = sqlite3.connect("database.db")
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS monthly_summary (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            year INTEGER,
            month INTEGER,
            platform TEXT,
            net_sales REAL,
            expenses REAL,
            earnings REAL,
            UNIQUE(year, month, platform)
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS sku_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            year INTEGER,
            month INTEGER,
            platform TEXT,
            sku TEXT,
            net_units REAL,
            net_sales REAL,
            earnings REAL,
            UNIQUE(year, month, platform, sku)
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS daily_sales (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT,
            year INTEGER,
            month INTEGER,
            platform TEXT,
            net_sales REAL,
            UNIQUE(date, platform)
        )
    """)

    conn.commit()
    conn.close()

init_db()

# ---------------- LOGIN ----------------

@app.route("/", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        if request.form["username"] == "irfan" and request.form["password"] == "irfan999":
            session["user"] = "irfan"
            return redirect("/platform/flipkart")
        else:
            return "Invalid credentials"
    return render_template("login.html")


@app.route("/logout")
def logout():
    session.clear()
    return redirect("/")

# ---------------- UPLOAD ----------------

@app.route("/upload-data", methods=["GET", "POST"])
def upload():

    if "user" not in session:
        return redirect("/")

    if request.method == "POST":

        file = request.files.get("file")
        platform = request.form.get("platform")

        if not file or not platform:
            return redirect("/upload-data")

        normalized_platform = platform.strip().capitalize()
        filepath = os.path.join(UPLOAD_FOLDER, file.filename)
        file.save(filepath)

        conn = sqlite3.connect("database.db")
        cursor = conn.cursor()

        # ===================== FLIPKART =====================
        if normalized_platform == "Flipkart":

            df = pd.read_excel(filepath, sheet_name="Orders P&L")
            df.columns = df.columns.str.strip()

            df["Order Date"] = pd.to_datetime(df["Order Date"], errors="coerce")
            df = df.dropna(subset=["Order Date"])

            df["Year"] = df["Order Date"].dt.year
            df["Month"] = df["Order Date"].dt.month

            df["Net Units"] = pd.to_numeric(df["Net Units"], errors="coerce").fillna(0)
            df["Accounted Net Sales (INR)"] = pd.to_numeric(df["Accounted Net Sales (INR)"], errors="coerce").fillna(0)
            df["Total Expenses (INR)"] = pd.to_numeric(df["Total Expenses (INR)"], errors="coerce").fillna(0)
            df["Net Earnings (INR)"] = pd.to_numeric(df["Net Earnings (INR)"], errors="coerce").fillna(0)

            unique_months = df[["Year", "Month"]].drop_duplicates()

            for _, row in unique_months.iterrows():
                cursor.execute("DELETE FROM monthly_summary WHERE year=? AND month=? AND platform=?",
                               (int(row["Year"]), int(row["Month"]), normalized_platform))
                cursor.execute("DELETE FROM sku_data WHERE year=? AND month=? AND platform=?",
                               (int(row["Year"]), int(row["Month"]), normalized_platform))
                cursor.execute("DELETE FROM daily_sales WHERE year=? AND month=? AND platform=?",
                               (int(row["Year"]), int(row["Month"]), normalized_platform))

            grouped = df.groupby(["Year", "Month"]).agg({
                "Accounted Net Sales (INR)": "sum",
                "Total Expenses (INR)": "sum",
                "Net Earnings (INR)": "sum"
            }).reset_index()

            for _, row in grouped.iterrows():
                cursor.execute("""
                    INSERT INTO monthly_summary
                    (year, month, platform, net_sales, expenses, earnings)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (
                    int(row["Year"]),
                    int(row["Month"]),
                    normalized_platform,
                    float(row["Accounted Net Sales (INR)"]),
                    float(abs(row["Total Expenses (INR)"])),
                    float(row["Net Earnings (INR)"])
                ))

            sku_grouped = df.groupby(["Year", "Month", "SKU Name"]).agg({
                "Net Units": "sum",
                "Accounted Net Sales (INR)": "sum",
                "Net Earnings (INR)": "sum"
            }).reset_index()

            for _, row in sku_grouped.iterrows():
                cursor.execute("""
                    INSERT OR REPLACE INTO sku_data
                    (year, month, platform, sku, net_units, net_sales, earnings)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (
                    int(row["Year"]),
                    int(row["Month"]),
                    normalized_platform,
                    str(row["SKU Name"]),
                    float(row["Net Units"]),
                    float(row["Accounted Net Sales (INR)"]),
                    float(row["Net Earnings (INR)"])
                ))

            daily_grouped = df.groupby("Order Date")["Accounted Net Sales (INR)"].sum().reset_index()

            for _, row in daily_grouped.iterrows():
                cursor.execute("""
                    INSERT OR REPLACE INTO daily_sales
                    (date, year, month, platform, net_sales)
                    VALUES (?, ?, ?, ?, ?)
                """, (
                    row["Order Date"].strftime("%Y-%m-%d"),
                    row["Order Date"].year,
                    row["Order Date"].month,
                    normalized_platform,
                    float(row["Accounted Net Sales (INR)"])
                ))

        # ===================== AMAZON =====================
        elif normalized_platform == "Amazon":

            df = pd.read_csv(filepath) if file.filename.lower().endswith(".csv") else pd.read_excel(filepath)

            df.columns = df.columns.str.strip()
            df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
            df = df.dropna(subset=["Date"])

            df["Year"] = df["Date"].dt.year
            df["Month"] = df["Date"].dt.month

            df["Ordered Product Sales"] = (
                df["Ordered Product Sales"]
                .astype(str)
                .str.replace("₹", "", regex=False)
                .str.replace(",", "", regex=False)
                .astype(float)
            )

            unique_months = df[["Year", "Month"]].drop_duplicates()

            for _, row in unique_months.iterrows():
                cursor.execute("DELETE FROM monthly_summary WHERE year=? AND month=? AND platform=?",
                               (int(row["Year"]), int(row["Month"]), normalized_platform))
                cursor.execute("DELETE FROM daily_sales WHERE year=? AND month=? AND platform=?",
                               (int(row["Year"]), int(row["Month"]), normalized_platform))

            grouped = df.groupby(["Year", "Month"])["Ordered Product Sales"].sum().reset_index()

            for _, row in grouped.iterrows():
                cursor.execute("""
                    INSERT INTO monthly_summary
                    (year, month, platform, net_sales, expenses, earnings)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (
                    int(row["Year"]),
                    int(row["Month"]),
                    normalized_platform,
                    float(row["Ordered Product Sales"]),
                    0,
                    float(row["Ordered Product Sales"])
                ))

            for _, row in df.iterrows():
                cursor.execute("""
                    INSERT OR REPLACE INTO daily_sales
                    (date, year, month, platform, net_sales)
                    VALUES (?, ?, ?, ?, ?)
                """, (
                    row["Date"].strftime("%Y-%m-%d"),
                    row["Date"].year,
                    row["Date"].month,
                    normalized_platform,
                    float(row["Ordered Product Sales"])
                ))

        # ===================== WOOCOMMERCE =====================
        elif normalized_platform == "Woocommerce":

            df = pd.read_csv(filepath) if file.filename.lower().endswith(".csv") else pd.read_excel(filepath)

            df.columns = df.columns.str.strip()
            df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
            df = df.dropna(subset=["Date"])

            df["Year"] = df["Date"].dt.year
            df["Month"] = df["Date"].dt.month

            df["Net sales amount"] = pd.to_numeric(df["Net sales amount"], errors="coerce").fillna(0)
            df["Refund amount"] = pd.to_numeric(df["Refund amount"], errors="coerce").fillna(0)

            unique_months = df[["Year", "Month"]].drop_duplicates()

            for _, row in unique_months.iterrows():
                cursor.execute("DELETE FROM monthly_summary WHERE year=? AND month=? AND platform=?",
                               (int(row["Year"]), int(row["Month"]), normalized_platform))
                cursor.execute("DELETE FROM daily_sales WHERE year=? AND month=? AND platform=?",
                               (int(row["Year"]), int(row["Month"]), normalized_platform))

            grouped = df.groupby(["Year", "Month"]).agg({
                "Net sales amount": "sum",
                "Refund amount": "sum"
            }).reset_index()

            for _, row in grouped.iterrows():
                net_sales = float(row["Net sales amount"])
                refund = float(row["Refund amount"])
                earnings = net_sales - refund

                cursor.execute("""
                    INSERT INTO monthly_summary
                    (year, month, platform, net_sales, expenses, earnings)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (
                    int(row["Year"]),
                    int(row["Month"]),
                    normalized_platform,
                    net_sales,
                    refund,
                    earnings
                ))

            for _, row in df.iterrows():
                cursor.execute("""
                    INSERT OR REPLACE INTO daily_sales
                    (date, year, month, platform, net_sales)
                    VALUES (?, ?, ?, ?, ?)
                """, (
                    row["Date"].strftime("%Y-%m-%d"),
                    row["Date"].year,
                    row["Date"].month,
                    normalized_platform,
                    float(row["Net sales amount"])
                ))

        # ===================== MEESHO =====================
        elif normalized_platform == "Meesho":

            df = pd.read_csv(filepath) if file.filename.lower().endswith(".csv") else pd.read_excel(filepath)

            df.columns = df.columns.str.strip()

            df["Order Date"] = pd.to_datetime(df["Order Date"], errors="coerce")
            df = df.dropna(subset=["Order Date"])

            df["Year"] = df["Order Date"].dt.year
            df["Month"] = df["Order Date"].dt.month

            df["Quantity"] = pd.to_numeric(df["Quantity"], errors="coerce").fillna(0)

            df["Supplier Discounted Price (Incl GST and Commision)"] = (
                df["Supplier Discounted Price (Incl GST and Commision)"]
                .astype(str)
                .str.replace("₹", "", regex=False)
                .str.replace(",", "", regex=False)
                .astype(float)
            )

            unique_months = df[["Year", "Month"]].drop_duplicates()

            for _, row in unique_months.iterrows():
                cursor.execute("DELETE FROM monthly_summary WHERE year=? AND month=? AND platform=?",
                               (int(row["Year"]), int(row["Month"]), normalized_platform))
                cursor.execute("DELETE FROM sku_data WHERE year=? AND month=? AND platform=?",
                               (int(row["Year"]), int(row["Month"]), normalized_platform))
                cursor.execute("DELETE FROM daily_sales WHERE year=? AND month=? AND platform=?",
                               (int(row["Year"]), int(row["Month"]), normalized_platform))

            grouped = df.groupby(["Year", "Month"])["Supplier Discounted Price (Incl GST and Commision)"].sum().reset_index()

            for _, row in grouped.iterrows():
                net_sales = float(row["Supplier Discounted Price (Incl GST and Commision)"])

                cursor.execute("""
                    INSERT INTO monthly_summary
                    (year, month, platform, net_sales, expenses, earnings)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (
                    int(row["Year"]),
                    int(row["Month"]),
                    normalized_platform,
                    net_sales,
                    0,
                    net_sales
                ))

            sku_grouped = df.groupby(["Year", "Month", "SKU"]).agg({
                "Quantity": "sum",
                "Supplier Discounted Price (Incl GST and Commision)": "sum"
            }).reset_index()

            for _, row in sku_grouped.iterrows():
                cursor.execute("""
                    INSERT OR REPLACE INTO sku_data
                    (year, month, platform, sku, net_units, net_sales, earnings)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (
                    int(row["Year"]),
                    int(row["Month"]),
                    normalized_platform,
                    str(row["SKU"]),
                    float(row["Quantity"]),
                    float(row["Supplier Discounted Price (Incl GST and Commision)"]),
                    float(row["Supplier Discounted Price (Incl GST and Commision)"])
                ))

            daily_grouped = df.groupby("Order Date")["Supplier Discounted Price (Incl GST and Commision)"].sum().reset_index()

            for _, row in daily_grouped.iterrows():
                cursor.execute("""
                    INSERT OR REPLACE INTO daily_sales
                    (date, year, month, platform, net_sales)
                    VALUES (?, ?, ?, ?, ?)
                """, (
                    row["Order Date"].strftime("%Y-%m-%d"),
                    row["Order Date"].year,
                    row["Order Date"].month,
                    normalized_platform,
                    float(row["Supplier Discounted Price (Incl GST and Commision)"])
                ))

                # ===================== SHOPIFY =====================
        elif normalized_platform == "Shopify":

            if file.filename.lower().endswith(".csv"):
                df = pd.read_csv(filepath)
            else:
                df = pd.read_excel(filepath)

            df.columns = df.columns.str.strip()

            df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
            df = df.dropna(subset=["Date"])

            df["Year"] = df["Date"].dt.year
            df["Month"] = df["Date"].dt.month

            df["Quantity"] = pd.to_numeric(df["Quantity"], errors="coerce").fillna(0)
            df["Net Sales (INR)"] = pd.to_numeric(df["Net Sales (INR)"], errors="coerce").fillna(0)

            unique_months = df[["Year", "Month"]].drop_duplicates()

            for _, row in unique_months.iterrows():
                cursor.execute("DELETE FROM monthly_summary WHERE year=? AND month=? AND platform=?",
                               (int(row["Year"]), int(row["Month"]), normalized_platform))
                cursor.execute("DELETE FROM sku_data WHERE year=? AND month=? AND platform=?",
                               (int(row["Year"]), int(row["Month"]), normalized_platform))
                cursor.execute("DELETE FROM daily_sales WHERE year=? AND month=? AND platform=?",
                               (int(row["Year"]), int(row["Month"]), normalized_platform))

            # Monthly Summary
            grouped = df.groupby(["Year", "Month"])["Net Sales (INR)"].sum().reset_index()

            for _, row in grouped.iterrows():
                net_sales = float(row["Net Sales (INR)"])

                cursor.execute("""
                    INSERT INTO monthly_summary
                    (year, month, platform, net_sales, expenses, earnings)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (
                    int(row["Year"]),
                    int(row["Month"]),
                    normalized_platform,
                    net_sales,
                    0,
                    net_sales
                ))

            # SKU Data
            sku_grouped = df.groupby(["Year", "Month", "SKU"]).agg({
                "Quantity": "sum",
                "Net Sales (INR)": "sum"
            }).reset_index()

            for _, row in sku_grouped.iterrows():
                cursor.execute("""
                    INSERT OR REPLACE INTO sku_data
                    (year, month, platform, sku, net_units, net_sales, earnings)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (
                    int(row["Year"]),
                    int(row["Month"]),
                    normalized_platform,
                    str(row["SKU"]),
                    float(row["Quantity"]),
                    float(row["Net Sales (INR)"]),
                    float(row["Net Sales (INR)"])
                ))

            # Daily Sales
            daily_grouped = df.groupby("Date")["Net Sales (INR)"].sum().reset_index()

            for _, row in daily_grouped.iterrows():
                cursor.execute("""
                    INSERT OR REPLACE INTO daily_sales
                    (date, year, month, platform, net_sales)
                    VALUES (?, ?, ?, ?, ?)
                """, (
                    row["Date"].strftime("%Y-%m-%d"),
                    row["Date"].year,
                    row["Date"].month,
                    normalized_platform,
                    float(row["Net Sales (INR)"])
                ))
        conn.commit()
        conn.close()

        return redirect(f"/platform/{normalized_platform.lower()}")

    return render_template("upload.html")

# ---------------- PLATFORM PAGE ----------------

@app.route("/platform/<platform>")
def platform_page(platform):

    if "user" not in session:
        return redirect("/")

    normalized_platform = platform.capitalize()

    conn = sqlite3.connect("database.db")
    cursor = conn.cursor()

    cursor.execute("""
        SELECT year, month, net_sales, earnings
        FROM monthly_summary
        WHERE LOWER(platform)=LOWER(?)
        ORDER BY year DESC, month DESC
    """, (normalized_platform,))

    data = cursor.fetchall()
    conn.close()

    return render_template("platform.html", data=data, platform=normalized_platform)

# ---------------- MONTH DASHBOARD ----------------

@app.route("/platform/<platform>/<int:year>/<int:month>")
def month_dashboard(platform, year, month):

    if "user" not in session:
        return redirect("/")

    normalized_platform = platform.capitalize()
    compare_year = request.args.get("compare_year")
    compare_month = request.args.get("compare_month")
    compare_data = None

    conn = sqlite3.connect("database.db")
    cursor = conn.cursor()

    cursor.execute("""
        SELECT net_sales, expenses, earnings
        FROM monthly_summary
        WHERE LOWER(platform)=LOWER(?) AND year=? AND month=?
    """, (normalized_platform, year, month))

    row = cursor.fetchone()
    if not row:
        return "No Data Found"

    net_sales, expenses, earnings = row
    margin = round((earnings / net_sales) * 100, 2) if net_sales else 0

    if compare_year and compare_month:
        cursor.execute("""
            SELECT net_sales, expenses, earnings
            FROM monthly_summary
            WHERE LOWER(platform)=LOWER(?) AND year=? AND month=?
        """, (normalized_platform, compare_year, compare_month))

        comp = cursor.fetchone()
        if comp:
            compare_data = {
                "net_sales": comp[0],
                "expenses": comp[1],
                "earnings": comp[2],
                "year": compare_year,
                "month": compare_month
            }

    cursor.execute("""
        SELECT date, net_sales
        FROM daily_sales
        WHERE LOWER(platform)=LOWER(?) AND year=? AND month=?
        ORDER BY date
    """, (normalized_platform, year, month))
    trend_data = cursor.fetchall()

    cursor.execute("""
        SELECT year, month
        FROM monthly_summary
        WHERE LOWER(platform)=LOWER(?)
        ORDER BY year DESC, month DESC
    """, (normalized_platform,))
    available_months = cursor.fetchall()

    cursor.execute("""
        SELECT sku, net_units
        FROM sku_data
        WHERE LOWER(platform)=LOWER(?) AND year=? AND month=?
        ORDER BY net_units DESC
        LIMIT 10
    """, (normalized_platform, year, month))
    top_units = cursor.fetchall()

    cursor.execute("""
        SELECT sku, net_sales
        FROM sku_data
        WHERE LOWER(platform)=LOWER(?) AND year=? AND month=?
        ORDER BY net_sales DESC
        LIMIT 10
    """, (normalized_platform, year, month))
    top_revenue = cursor.fetchall()

    conn.close()

    return render_template(
        "month_dashboard.html",
        net_sales=net_sales,
        expenses=expenses,
        earnings=earnings,
        margin=margin,
        compare_data=compare_data,
        trend_data=trend_data,
        available_months=available_months,
        top_units=top_units,
        top_revenue=top_revenue,
        year=year,
        month=month,
        platform=normalized_platform
    )

if __name__ == "__main__":
    app.run()