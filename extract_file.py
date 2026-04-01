import pandas as pd

df = pd.read_excel(fr"C:\Users\User\Documents\SAP\MASTER DATA TRANSAKSI FINAL v2.xlsx")

# rows_per_file = len(df) // 15 + (len(df) % 15 > 0)

rows_per_file = 10000
total_rows = len(df)
total_files = (total_rows // rows_per_file) + (total_rows % rows_per_file > 0)

for i in range(total_files):
    start = i * rows_per_file
    end = start + rows_per_file
    chunk = df.iloc[start:end]
    chunk.to_excel(fr"C:\Users\User\Documents\SAP\Output\MASTER DATA TRANSAKSI FINAL_{i+1}.xlsx", index=False)

print ("Done")


# import pandas as pd

# # Load file Excel
# df = pd.read_excel(fr"C:\Users\User\Documents\SAP\MASTER DATA TRANSAKSI FINAL.xlsx")

# # Pastikan kolom tanggal dalam format datetime
# df["Tanggal"] = pd.to_datetime(df["Tanggal_Transaksi"])

# # Ambil semua tanggal unik
# unique_dates = df["Tanggal"].dt.date.unique()

# # Loop dan simpan per tanggal
# for date in unique_dates:
#     chunk = df[df["Tanggal"].dt.date == date]
#     chunk.to_excel(fr"C:\Users\User\Documents\SAP\Output\data_{date}.xlsx", index=False)
