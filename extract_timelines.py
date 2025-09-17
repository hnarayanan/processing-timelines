#!/usr/bin/env python3
"""
Extract + normalise UK naturalisation timelines from Reddit-style JSON into TSV.

Key points:
- One API call per comment; writes ONE ROW AT A TIME (fail-safe if a later call errors).
- First TSV column is `Comment ID` so you can match and skip in future runs.
- Heavy instructions in the prompt; minimal code logic.
- Dates must be ISO "YYYY-MM-DD" or the literal "N/A".
- Eligibility is one of a small, robust set (see prompt) with optional " (+ Marriage)" suffix.
- Application Method ∈ {Online, Paper, Other}.
- Non-timeline comments are skipped.

Usage:
  OPENAI_API_KEY=... python extract_timelines.py input.json output.tsv --model gpt-4o-mini

Env:
  OPENAI_MODEL (default: gpt-5)
  RATE_LIMIT_DELAY_SEC (default: 0.3)
"""

import argparse
import json
import os
import sys
import time
from typing import Any, Set

DEFAULT_MODEL = os.environ.get("OPENAI_MODEL", "gpt-5")
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
])

def sanity_normalise_eligibility(value: str) -> str:
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
    val = str(x or "").strip()
    if val == "N/A":
        return val
    # Light format check; rely on model for parsing/formatting
    if len(val) == 10 and val[4] == "-" and val[7] == "-":
        return val
    return "N/A"

def read_cached_ids(tsv_path: str) -> Set[str]:
    ids: Set[str] = set()
    if not tsv_path or not os.path.exists(tsv_path):
        return ids
    try:
        with open(tsv_path, "r", encoding="utf-8") as f:
            first = True
            for line in f:
                if first:
                    first = False
                    continue  # skip header
                parts = line.rstrip("\n").split("\t")
                if parts and parts[0]:
                    ids.add(parts[0])
    except Exception as e:
        print(f"[warn] Could not read cache TSV '{tsv_path}': {e}", file=sys.stderr)
    return ids

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("input_json", help="Path to input JSON (with top-level 'comments' array).")
    parser.add_argument("output_tsv", help="Path to write/append the TSV.")
    parser.add_argument("--model", default=DEFAULT_MODEL, help=f"OpenAI model (default: {DEFAULT_MODEL})")
    parser.add_argument("--cache-tsv", default=None, help="Optional TSV to use as cache of processed comment_ids.")
    args = parser.parse_args()

    # Load input
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

    # Build the processed cache from:
    # 1) --cache-tsv (if provided)
    # 2) existing output file (if exists)
    processed_ids: Set[str] = set()
    if args.cache_tsv:
        processed_ids |= read_cached_ids(args.cache_tsv)
    if os.path.exists(args.output_tsv):
        processed_ids |= read_cached_ids(args.output_tsv)

    # Open output TSV (append if exists, else create and write header)
    new_file = not os.path.exists(args.output_tsv)
    try:
        out_f = open(args.output_tsv, "a" if not new_file else "w", encoding="utf-8", newline="")
    except Exception as e:
        print(f"Failed to open output TSV for writing: {e}", file=sys.stderr)
        sys.exit(1)

    with out_f:
        if new_file:
            out_f.write(TSV_HEADER + "\n")
            out_f.flush()

        # OpenAI client
        try:
            from openai import OpenAI
        except Exception:
            print("Please install the official OpenAI Python SDK: pip install openai", file=sys.stderr)
            sys.exit(1)

        client = OpenAI()

        total_written = 0
        total_skipped_cached = 0

        for idx, c in enumerate(comments, start=1):
            print(c)
            body = c.get("body", "")
            comment_id = c.get("comment_id") or c.get("name") or c.get("id") or f"c{idx:06d}"

            if not isinstance(body, str) or not body.strip():
                continue

            # Skip if we've already processed this comment_id
            if comment_id in processed_ids:
                print("--- Skipping this comment ---")
                total_skipped_cached += 1
                continue

            try:
                print("--- Processing this comment ---")
                resp = client.chat.completions.create(
                    model=args.model,
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
                if RATE_LIMIT_DELAY_SEC:
                    time.sleep(RATE_LIMIT_DELAY_SEC)
                continue

            if parsed.get("skip") is True:
                if RATE_LIMIT_DELAY_SEC:
                    time.sleep(RATE_LIMIT_DELAY_SEC)
                continue

            eligibility_out = sanity_normalise_eligibility(parsed.get("eligibility", "ILR"))
            application_method = str(parsed.get("application_method", "Online")).strip().title()
            if application_method not in {"Online", "Paper", "Other"}:
                application_method = "Online"

            row = [
                comment_id,
                eligibility_out,
                application_method,
                sanity_norm_date(parsed.get("application_date")),
                sanity_norm_date(parsed.get("biometric_date")),
                sanity_norm_date(parsed.get("approval_date")),
                sanity_norm_date(parsed.get("ceremony_date")),
            ]

            out_f.write("\t".join(row) + "\n")
            out_f.flush()
            processed_ids.add(comment_id)
            total_written += 1

            if RATE_LIMIT_DELAY_SEC:
                time.sleep(RATE_LIMIT_DELAY_SEC)

        print(f"Wrote {total_written} new row(s) to {args.output_tsv} (skipped {total_skipped_cached} cached).")

if __name__ == "__main__":
    main()
