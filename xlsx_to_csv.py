import pandas as pd
import sys
import os

def convert(xlsx_path):
    xl = pd.ExcelFile(xlsx_path)
    base = os.path.splitext(xlsx_path)[0]

    for sheet in xl.sheet_names:
        df = pd.read_excel(xl, sheet_name=sheet)
        if len(xl.sheet_names) == 1:
            out = f"{base}.csv"
        else:
            out = f"{base}_{sheet}.csv"
        df.to_csv(out, index=False)
        print(f"  -> {out} ({len(df)} rows)")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python3 xlsx_to_csv.py file1.xlsx file2.xlsx ...")
        sys.exit(1)

    for f in sys.argv[1:]:
        print(f"Converting: {f}")
        convert(f)
    print("Done!")
