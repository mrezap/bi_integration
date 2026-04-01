import pandas as pd
from datetime import datetime

excel_file = fr"Z:\@Database\SC CSV\SC Master 2025.xlsx"
output_file = fr"Z:\@Database\SC CSV\SC Master 2025.csv"

print("Processing..")

df = pd.read_excel(excel_file)

# print(f"Kolom: {df.columns.tolist()}")
# print(f"Tipe data sebelum: {df.dtypes}")

if 'Date' in df.columns:
    df['Date'] = pd.to_datetime(df['Date'], format='%d/%m/%Y', errors='coerce')
    null_dates = df['Date'].isna().sum()
    if null_dates > 0:
        print(f"Warning: {null_dates} failed to parsing date value!")

    df['Date'] = df['Date'].dt.strftime('%Y-%m-%d')

# print(f"Tipe data setelah: {df.dtypes}")
# print(f"Preview data:")
# print(df.head())

df.to_csv(output_file, index=False, date_format='%Y-%m-%d')
print(f"Done, file store in: {output_file}")

