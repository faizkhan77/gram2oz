# gram2oz

**Convert per-gram gold rates to per-ounce**, process them in chunks, and export the result to CSV and Excel.  
Supports efficient processing of large files with MySQL backend integration.

---

## 📦 Features

- Converts gold rate columns from **grams to ounces**.
- Chunked processing for large CSV files.
- Export to:
  - MySQL table
  - CSV file
  - Excel file (`.xlsx`)
- Progress bar and colored logging for better tracking.

---

## 🚀 Getting Started

### 1. Clone the Repository

```bash
git clone https://github.com/faizkhan77/gram2oz.git
cd gram2oz
```

### 2. Create & Activate Virtual Environment
```bash
# Create virtual environment
python -m venv venv

# Activate it (Windows)
venv\Scripts\activate

# Activate it (macOS/Linux)
source venv/bin/activate
```

### 3. Install Dependencies
```bash
pip install -r requirements.txt
```

### 🔧 Usage
**Example command:**
```bash
python convert_gold_rates.py \
  --input gold.csv \
  --output-csv gold_rates_converted.csv \
  --output-xlsx gold_rates_converted.xlsx \
  --table gold_rates
```

| Argument        | Required | Description                                                                     |
| --------------- | -------- | ------------------------------------------------------------------------------- |
| `--input`       | ✅        | Path to the input CSV file containing per-gram gold rates.                      |
| `--output-csv`  | ❌        | Path to save the converted output CSV with per-ounce columns.                   |
| `--output-xlsx` | ❌        | Path to save the converted data in Excel format (writes entire file to memory). |
| `--table`       | ❌        | MySQL table name to write the data to. Default is `gold_rates`.                 |
| `--chunksize`   | ❌        | Number of rows to process per chunk (default: 50000). Helpful for large files.  |


Note: The script uses a MySQL database. Make sure your database is running and credentials are set correctly in the script under DATABASE_URL.

### 🧪 Sample Conversion Logic

All values in columns like buyRateUSD, sellRateAED, etc., are converted using:
```bash
1 ounce = 31.1035 grams
```

