#!/usr/bin/env python3
"""
Extract + normalise UK naturalisation timelines from Reddit-style JSON into TSV.
Handles comment updates by tracking body hashes and intelligently merging dates.

Usage:
  OPENAI_API_KEY=... python extract_timelines.py input.json output.tsv --model gpt-4o-mini

Env:
  OPENAI_MODEL (default: gpt-5.1)
  RATE_LIMIT_DELAY_SEC (default: 0.1)
"""

import argparse
import hashlib
import json
import os
import shutil
import sys
import time
from datetime import datetime
from typing import Any, Dict, Optional, Set

DEFAULT_MODEL = os.environ.get("OPENAI_MODEL", "gpt-5.1")
RATE_LIMIT_DELAY_SEC = float(os.environ.get("RATE_LIMIT_DELAY_SEC", "0.1"))

SYSTEM_PROMPT = """You are a careful information normaliser. Extract exactly ONE timeline row
from a single Reddit-style comment body. Many comments are messy or include edits; you must
reliably pick the latest stated values and normalise them.

OUTPUT FORMAT (JSON object, no extra fields):
{
  "eligibility": "<string>",
  "application_method": "<Online|Paper|Other>",
  "application_date": "<YYYY-MM-DD|N/A>",
  "biometric_date": "<YYYY-MM-DD|N/A>",
  "approval_date": "<YYYY-MM-DD|N/A>",
  "ceremony_date": "<YYYY-MM-DD|N/A>",
  "skip": <true|false>
}

STRICT RULES:
1) If the comment does NOT clearly contain a citizenship timeline with at least an eligibility AND one date,
   return skip=true and put "N/A" for all date fields. Otherwise skip=false.

2) ELIGIBILITY (canonical, concise):
   Choose the SINGLE best base from this list, based on the comment:
     - "ILR"                (Indefinite Leave to Remain; includes Tier 2/Skilled Worker→ILR, Ancestry→ILR, Refugee→ILR, Global Talent→ILR, etc.)
     - "EUSS"               (EU Settlement Scheme / Settled Status)
     - "MN1 (Child)"        (registration of a minor under MN1)
     - "Form T"             (born in UK, 10 years’ residence route)
     - "BNO"                (British National (Overseas) route)
     - "Armed Forces"       (HM Forces routes)
   Then, if clearly and explicitly applicable, append ONE or more of these suffixes (in this order):
     - " (+ Marriage)"   – spouse of a British citizen / British spouse route
     - " (+ DV)"         – Domestic Violence concession/route (e.g., ILRDV)
     - " (+ Refugee)"    – refugee route stated explicitly
   Examples: "ILR", "ILR (+ Marriage)", "MN1 (Child)", "Form T", "Armed Forces", "EUSS (+ Marriage)".

   Keep it short; do not include extra descriptors (visa history, years, councils, etc.) in the final eligibility string.

3) APPLICATION METHOD:
   - Map to exactly one of: Online, Paper, Other.
   - Treat “online via solicitor / through solicitor portal / TLS upload” as Online.
   - If unspecified but implied, default to Online.

4) DATES:
   - Normalise dates to ISO "YYYY-MM-DD".
   - Accept and convert formats like "22/01/2025", "22/01/25" (assume 2000s), "22 Jan 2025", "January 22, 2025", "22-01-2025".
   - The thread is UK-centric: when parsing numeric dates like 03/04/2025, interpret as DD/MM/YYYY.
   - If a field is missing, unknown, "TBC", "pending", "N/A", or only a month with no day → use the literal "N/A".
   - If multiple dates are mentioned (e.g., edits), use the latest update in the comment body (last mention wins).
   - Ignore times (keep only the date).

5) ROBUSTNESS:
   - Comments may contain chatter or extra lines; extract only the six fields above.
   - Never include free text in date fields; only "YYYY-MM-DD" or "N/A".
   - Never add extra properties to the JSON output.

Return ONLY the JSON object (no prose).
"""

USER_PROMPT_TEMPLATE = """COMMENT BODY (verbatim):

{body}

Please return the JSON object as specified. Remember:
- eligibility must be one of: ILR, EUSS, MN1 (Child), Form T, BNO, Armed Forces
  (+ optional suffixes: " (+ Marriage)", " (+ DV)", " (+ Refugee)")
- application_method ∈ {{Online, Paper, Other}}
- all dates → "YYYY-MM-DD" or "N/A"
- choose the *latest* values if there are edits/updates
- set "skip": true if this isn't actually a timeline
"""

TSV_HEADER = "\t".join([
    "Comment ID",
    "Eligibility",
    "Application Method",
    "Application Date",
    "Biometric Date",
    "Approval Date",
    "Ceremony Date",
    "Body Hash",
])


class TimelineRow:
    """Represents a single timeline row with all fields."""

    def __init__(
        self,
        comment_id: str,
        eligibility: str,
        application_method: str,
        application_date: str,
        biometric_date: str,
        approval_date: str,
        ceremony_date: str,
        body_hash: str = "",
    ):
        self.comment_id = comment_id
        self.eligibility = eligibility
        self.application_method = application_method
        self.application_date = application_date
        self.biometric_date = biometric_date
        self.approval_date = approval_date
        self.ceremony_date = ceremony_date
        self.body_hash = body_hash

    def to_tsv_row(self) -> str:
        """Convert to TSV row string."""
        return "\t".join([
            self.comment_id,
            self.eligibility,
            self.application_method,
            self.application_date,
            self.biometric_date,
            self.approval_date,
            self.ceremony_date,
            self.body_hash,
        ])

    @classmethod
    def from_tsv_row(cls, line: str) -> Optional['TimelineRow']:
        """Parse a TSV row into a TimelineRow object."""
        parts = line.rstrip("\n").split("\t")
        if len(parts) < 7:
            return None

        # Handle optional body_hash column (backwards compatibility)
        body_hash = parts[7] if len(parts) > 7 else ""

        return cls(
            comment_id=parts[0],
            eligibility=parts[1],
            application_method=parts[2],
            application_date=parts[3],
            biometric_date=parts[4],
            approval_date=parts[5],
            ceremony_date=parts[6],
            body_hash=body_hash,
        )

    def merge_with(self, other: 'TimelineRow') -> 'TimelineRow':
        """
        Merge this row with another, intelligently combining dates.
        Strategy: Keep existing dates, fill in N/A values with new dates.
        For non-date fields, prefer the newer data.
        """
        return TimelineRow(
            comment_id=self.comment_id,
            eligibility=other.eligibility,  # Use newer eligibility
            application_method=other.application_method,  # Use newer method
            application_date=self._merge_date(self.application_date, other.application_date),
            biometric_date=self._merge_date(self.biometric_date, other.biometric_date),
            approval_date=self._merge_date(self.approval_date, other.approval_date),
            ceremony_date=self._merge_date(self.ceremony_date, other.ceremony_date),
            body_hash=other.body_hash,  # Update to new hash
        )

    @staticmethod
    def _merge_date(old_date: str, new_date: str) -> str:
        """
        Merge two date values intelligently.
        - If old is a real date, keep it (existing data is preserved)
        - If old is N/A and new is a real date, use new (fill in gaps)
        - Otherwise use N/A
        """
        if old_date and old_date != "N/A" and old_date.strip():
            # Old date exists and is valid, keep it
            return old_date
        elif new_date and new_date != "N/A" and new_date.strip():
            # Old was N/A but new has a date, use new
            return new_date
        else:
            # Both are N/A or invalid
            return "N/A"


def compute_body_hash(body: str) -> str:
    """Compute a hash of the comment body to detect changes."""
    return hashlib.sha256(body.encode('utf-8')).hexdigest()[:16]


def sanity_normalise_eligibility(value: str) -> str:
    """Normalize eligibility value to standard form."""
    v = (value or "").strip()
    upper = v.upper()

    # Detect base
    if "EUSS" in upper or "SETTLED STATUS" in upper or "EU SETTLED" in upper:
        base = "EUSS"
    elif "MN1" in upper:
        base = "MN1"
    elif "FORM T" in upper:
        base = "Form T"
    elif "ARMED" in upper:
        base = "Armed Forces"
    elif "BNO" in upper:
        base = "BNO"
    else:
        base = "ILR"

    # Marriage suffix?
    suffix = " (+ Marriage)" if (
        "MARRIAGE" in upper or "MARRIED TO BRITISH" in upper
        or "BRITISH SPOUSE" in upper or "SPOUSE OF A BRITISH" in upper
        or "(+ MARRIAGE)" in upper
    ) else ""

    return base + suffix


def sanity_norm_date(x: Any) -> str:
    """Normalize date value to YYYY-MM-DD or N/A."""
    val = str(x or "").strip()
    if val == "N/A":
        return val
    # Light format check; rely on model for parsing/formatting
    if len(val) == 10 and val[4] == "-" and val[7] == "-":
        return val
    return "N/A"


def read_existing_data(tsv_path: str) -> Dict[str, TimelineRow]:
    """Read existing TSV data into a dictionary keyed by comment_id."""
    data: Dict[str, TimelineRow] = {}

    if not tsv_path or not os.path.exists(tsv_path):
        return data

    try:
        with open(tsv_path, "r", encoding="utf-8") as f:
            first = True
            for line in f:
                if first:
                    first = False
                    continue  # skip header

                row = TimelineRow.from_tsv_row(line)
                if row and row.comment_id:
                    data[row.comment_id] = row
    except Exception as e:
        print(f"[warn] Could not read existing TSV '{tsv_path}': {e}", file=sys.stderr)

    return data


def process_comment(comment_id: str, body: str, model: str, client) -> Optional[TimelineRow]:
    """
    Process a single comment using the OpenAI API.
    Returns a TimelineRow if successful, None if skipped or error.
    """
    try:
        resp = client.chat.completions.create(
            model=model,
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": USER_PROMPT_TEMPLATE.format(body=body)},
            ],
        )
        content = resp.choices[0].message.content
        parsed = json.loads(content)
    except Exception as e:
        print(f"[warn] Comment {comment_id}: could not parse model JSON ({e}).", file=sys.stderr)
        try:
            raw = resp.choices[0].message.content
            print(f"[warn] Raw model output:\n{raw}", file=sys.stderr)
        except Exception:
            pass
        return None

    if parsed.get("skip") is True:
        return None

    eligibility_out = sanity_normalise_eligibility(parsed.get("eligibility", "ILR"))
    application_method = str(parsed.get("application_method", "Online")).strip().title()
    if application_method not in {"Online", "Paper", "Other"}:
        application_method = "Online"

    body_hash = compute_body_hash(body)

    return TimelineRow(
        comment_id=comment_id,
        eligibility=eligibility_out,
        application_method=application_method,
        application_date=sanity_norm_date(parsed.get("application_date")),
        biometric_date=sanity_norm_date(parsed.get("biometric_date")),
        approval_date=sanity_norm_date(parsed.get("approval_date")),
        ceremony_date=sanity_norm_date(parsed.get("ceremony_date")),
        body_hash=body_hash,
    )


def write_all_data(tsv_path: str, data: Dict[str, TimelineRow], create_backup: bool = True):
    """Write all timeline data to TSV, optionally creating a backup first."""
    if create_backup and os.path.exists(tsv_path):
        backup_path = f"{tsv_path}.backup.{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        try:
            shutil.copy2(tsv_path, backup_path)
            print(f"✓ Created backup: {backup_path}")
        except Exception as e:
            print(f"[warn] Could not create backup: {e}", file=sys.stderr)
    try:
        with open(tsv_path, "w", encoding="utf-8", newline="") as f:
            f.write(TSV_HEADER + "\n")
            # Sort by comment_id for consistent output
            for comment_id in sorted(data.keys()):
                f.write(data[comment_id].to_tsv_row() + "\n")
        print(f"✓ Wrote {len(data)} rows to {tsv_path}")
    except Exception as e:
        print(f"[error] Failed to write TSV: {e}", file=sys.stderr)
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(
        description="Extract UK naturalisation timelines from Reddit JSON, tracking updates."
    )
    parser.add_argument("input_json", help="Path to input JSON (with top-level 'comments' array).")
    parser.add_argument("output_tsv", help="Path to write/update the TSV.")
    parser.add_argument("--model", default=DEFAULT_MODEL, help=f"OpenAI model (default: {DEFAULT_MODEL})")
    parser.add_argument("--no-backup", action="store_true", help="Skip creating backup before rewriting TSV")
    args = parser.parse_args()

    # Load input JSON
    try:
        with open(args.input_json, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        print(f"Failed to read input JSON: {e}", file=sys.stderr)
        sys.exit(1)

    comments = data.get("comments") or []
    if not isinstance(comments, list):
        print("Input JSON missing 'comments' array.", file=sys.stderr)
        sys.exit(1)

    # Read existing data
    existing_data = read_existing_data(args.output_tsv)
    print(f"📚 Loaded {len(existing_data)} existing timeline(s)")

    # Load skipped cache (non-timeline comments)
    skipped_cache = {}
    skipped_file = f"{args.output_tsv}.skipped"
    if os.path.exists(skipped_file):
        try:
            with open(skipped_file, "r") as f:
                for line in f:
                    parts = line.strip().split("\t")
                    if len(parts) == 2:
                        skipped_cache[parts[0]] = parts[1]
        except Exception as e:
            print(f"[warn] Could not read skipped cache: {e}", file=sys.stderr)

    # Initialize OpenAI client
    try:
        from openai import OpenAI
    except Exception:
        print("Please install the official OpenAI Python SDK: pip install openai", file=sys.stderr)
        sys.exit(1)

    client = OpenAI()

    # Create backup BEFORE we start processing
    if not args.no_backup and os.path.exists(args.output_tsv):
        backup_path = f"{args.output_tsv}.backup.{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        try:
            shutil.copy2(args.output_tsv, backup_path)
            print(f"✓ Created backup: {backup_path}")
        except Exception as e:
            print(f"[warn] Could not create backup: {e}", file=sys.stderr)

    # Open in-progress file for incremental writes (CRASH-SAFE)
    inprogress_path = f"{args.output_tsv}.inprogress"
    try:
        inprogress_file = open(inprogress_path, "w", encoding="utf-8", newline="")
        inprogress_file.write(TSV_HEADER + "\n")
        inprogress_file.flush()
    except Exception as e:
        print(f"Failed to open in-progress file: {e}", file=sys.stderr)
        sys.exit(1)

    # Track which comments we've written to inprogress file
    written_to_inprogress = set()

    # Tracking counters
    stats = {
        "new": 0,
        "updated": 0,
        "unchanged": 0,
        "skipped": 0,
        "errors": 0,
    }

    try:
        # Process each comment
        for idx, c in enumerate(comments, start=1):
            body = c.get("body", "")
            comment_id = c.get("comment_id") or c.get("name") or c.get("id") or f"c{idx:06d}"

            if not isinstance(body, str) or not body.strip():
                continue

            # Skip deleted/removed - preserve old data if exists
            if body in ("[deleted]", "[removed]"):
                if comment_id in existing_data:
                    if comment_id not in written_to_inprogress:
                        inprogress_file.write(existing_data[comment_id].to_tsv_row() + "\n")
                        inprogress_file.flush()
                        written_to_inprogress.add(comment_id)
                continue

            body_hash = compute_body_hash(body)
            existing_row = existing_data.get(comment_id)

            # Check if already known non-timeline
            if comment_id in skipped_cache and skipped_cache[comment_id] == body_hash:
                stats["skipped"] += 1
                continue

            # Determine if we need to process this comment
            should_process = False
            reason = ""

            if existing_row is None:
                should_process = True
                reason = "new comment"
            elif existing_row.body_hash != body_hash:
                should_process = True
                reason = "comment edited"
            else:
                # Already processed and unchanged - write existing row to inprogress
                stats["unchanged"] += 1
                if idx % 50 == 0:
                    print(f"[{idx}/{len(comments)}] {comment_id}: unchanged")

                # Write unchanged row to inprogress file
                if comment_id not in written_to_inprogress:
                    inprogress_file.write(existing_row.to_tsv_row() + "\n")
                    inprogress_file.flush()
                    written_to_inprogress.add(comment_id)
                continue

            print(f"[{idx}/{len(comments)}] {comment_id}: processing ({reason})")

            # Process the comment
            new_row = process_comment(comment_id, body, args.model, client)

            if new_row is None:
                stats["skipped"] += 1
                skipped_cache[comment_id] = body_hash  # Remember non-timeline
                # Remove from data if it was previously there but now should be skipped
                if comment_id in existing_data:
                    del existing_data[comment_id]
                # Don't write to inprogress file (it's being removed)
            else:
                if existing_row is None:
                    # Brand new entry
                    existing_data[comment_id] = new_row
                    stats["new"] += 1
                    print(f"  → Added new timeline")
                else:
                    # Merge with existing data
                    merged_row = existing_row.merge_with(new_row)
                    existing_data[comment_id] = merged_row
                    new_row = merged_row  # Use merged row for writing
                    stats["updated"] += 1
                    print(f"  → Updated timeline (merged dates)")

                # Write to inprogress file IMMEDIATELY (crash-safe!)
                if comment_id not in written_to_inprogress:
                    inprogress_file.write(new_row.to_tsv_row() + "\n")
                    inprogress_file.flush()
                    written_to_inprogress.add(comment_id)

            # Rate limiting
            if RATE_LIMIT_DELAY_SEC:
                time.sleep(RATE_LIMIT_DELAY_SEC)

        # Write any remaining rows that weren't in the input comments but are in existing_data
        for comment_id, row in existing_data.items():
            if comment_id not in written_to_inprogress:
                inprogress_file.write(row.to_tsv_row() + "\n")
                written_to_inprogress.add(comment_id)

    finally:
        # Always close the inprogress file
        inprogress_file.close()

    # Now sort the inprogress file and write final output
    print("\n" + "="*60)
    print("📝 Finalizing output (sorting by comment ID)...")

    # Write skipped cache
    try:
        with open(skipped_file, "w") as f:
            for cid in sorted(skipped_cache.keys()):
                f.write(f"{cid}\t{skipped_cache[cid]}\n")
    except Exception as e:
        print(f"[warn] Could not write skipped cache: {e}", file=sys.stderr)

    try:
        # Read all rows from inprogress file
        final_data = {}
        with open(inprogress_path, "r", encoding="utf-8") as f:
            first = True
            for line in f:
                if first:
                    first = False
                    continue  # skip header
                row = TimelineRow.from_tsv_row(line)
                if row and row.comment_id:
                    final_data[row.comment_id] = row

        # Write sorted final output
        with open(args.output_tsv, "w", encoding="utf-8", newline="") as f:
            f.write(TSV_HEADER + "\n")
            for comment_id in sorted(final_data.keys()):
                f.write(final_data[comment_id].to_tsv_row() + "\n")

        # Remove inprogress file
        os.remove(inprogress_path)
        print(f"✓ Wrote {len(final_data)} rows to {args.output_tsv}")

    except Exception as e:
        print(f"[error] Failed to finalize output: {e}", file=sys.stderr)
        print(f"[info] Your data is safe in: {inprogress_path}", file=sys.stderr)
        sys.exit(1)

    # Print summary
    print("\n📊 Summary:")
    print(f"  New timelines added:        {stats['new']}")
    print(f"  Existing timelines updated: {stats['updated']}")
    print(f"  Unchanged timelines:        {stats['unchanged']}")
    print(f"  Non-timeline comments:      {stats['skipped']}")
    print(f"  Total timelines in TSV:     {len(final_data)}")
    print("\n✅ Done!")


if __name__ == "__main__":
    main()
