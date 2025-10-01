import pandas as pd
import numpy as np

KEY = "Comment ID"
DATE_COLS = ["Application Date", "Biometric Date", "Approval Date", "Ceremony Date"]

local_sheet = pd.read_csv(
    "processing_timelines.tsv",
    sep="\t",
    parse_dates=DATE_COLS,
    na_values=["N/A"]
)
google_sheet = pd.read_excel("Untitled spreadsheet.xlsx")

local_sheet["_row_pos"] = np.arange(len(local_sheet))

L = local_sheet.set_index(KEY)
G = google_sheet.set_index(KEY)

common = L.index.intersection(G.index)

def neq(a, b):
    return a.ne(b) & ~(a.isna() & b.isna())

for col in L.columns:
    if col not in G.columns:  # guard if sheets drifted
        continue
    mask = neq(L.loc[common, col], G.loc[common, col])
    idx_to_update = common[mask]
    if len(idx_to_update):
        L.loc[idx_to_update, col] = G.loc[idx_to_update, col]

out = (
    L.reset_index()                       # bring KEY back as a column
     .sort_values("_row_pos")             # restore original row order
     .drop(columns="_row_pos")
)

for c in DATE_COLS:
    out[c] = out[c].dt.strftime("%Y-%m-%d").fillna("N/A")


same_order = out[KEY].tolist() == local_sheet.sort_values("_row_pos")[KEY].tolist()
print("Row order preserved?", same_order)
if not same_order:
    a = local_sheet.sort_values("_row_pos")[KEY].tolist()
    b = out[KEY].tolist()
    diffs = [(i, a[i], b[i]) for i in range(min(len(a), len(b))) if a[i] != b[i]]
    print("First 10 order diffs (pos, original, output):", diffs[:10])

out.to_csv("processing_timelines.updated.tsv", sep="\t", index=False)
print("Wrote processing_timelines.updated.tsv")
