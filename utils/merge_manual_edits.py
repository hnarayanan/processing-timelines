#!/usr/bin/env python3
"""
Merge manually-curated old TSV with new TSV, preferring old values.
"""

def prefer_old(old_val, new_val):
    """Prefer old value if it's not empty/N/A, otherwise use new."""
    if old_val and old_val != "N/A" and old_val.strip():
        return old_val
    return new_val

# Read old file (without Body Hash column)
old_data = {}
with open("processing_timelines.tsv.backup.20251111_194715", "r") as f:
    header = next(f)
    for line in f:
        parts = line.strip().split("\t")
        if len(parts) >= 7:
            comment_id = parts[0]
            old_data[comment_id] = {
                "eligibility": parts[1],
                "method": parts[2],
                "app_date": parts[3],
                "bio_date": parts[4],
                "approval_date": parts[5],
                "ceremony_date": parts[6]
            }

# Read new file (with Body Hash column)
new_data = {}
with open("processing_timelines.tsv", "r") as f:
    header = next(f)
    for line in f:
        parts = line.strip().split("\t")
        if len(parts) >= 8:
            comment_id = parts[0]
            new_data[comment_id] = {
                "eligibility": parts[1],
                "method": parts[2],
                "app_date": parts[3],
                "bio_date": parts[4],
                "approval_date": parts[5],
                "ceremony_date": parts[6],
                "body_hash": parts[7]
            }

# Merge: prefer old values, keep new body_hash
merged = {}
for comment_id, new_row in new_data.items():
    if comment_id in old_data:
        old_row = old_data[comment_id]
        merged[comment_id] = {
            "eligibility": prefer_old(old_row["eligibility"], new_row["eligibility"]),
            "method": prefer_old(old_row["method"], new_row["method"]),
            "app_date": prefer_old(old_row["app_date"], new_row["app_date"]),
            "bio_date": prefer_old(old_row["bio_date"], new_row["bio_date"]),
            "approval_date": prefer_old(old_row["approval_date"], new_row["approval_date"]),
            "ceremony_date": prefer_old(old_row["ceremony_date"], new_row["ceremony_date"]),
            "body_hash": new_row["body_hash"]
        }
    else:
        # New comment not in old file, keep as-is
        merged[comment_id] = new_row

# Write merged output
with open("processing_timelines_merged.tsv", "w") as f:
    f.write("Comment ID\tEligibility\tApplication Method\tApplication Date\tBiometric Date\tApproval Date\tCeremony Date\tBody Hash\n")
    for comment_id in sorted(merged.keys()):
        row = merged[comment_id]
        f.write(f"{comment_id}\t{row['eligibility']}\t{row['method']}\t{row['app_date']}\t{row['bio_date']}\t{row['approval_date']}\t{row['ceremony_date']}\t{row['body_hash']}\n")

print(f"Merged {len(merged)} rows to processing_timelines_merged.tsv")
print(f"  Old file had: {len(old_data)} rows")
print(f"  New file had: {len(new_data)} rows")
