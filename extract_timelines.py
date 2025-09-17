#!/usr/bin/env python3
"""
Extract + normalise UK naturalisation timelines from Reddit-style JSON into TSV.

Design:
- Minimal code; heavy, explicit instructions in the prompt.
- One API call per comment (robust to large threads).
- Strictly output 6 fields (Eligibility, Application Method, Application Date, Biometric Date, Approval Date, Ceremony Date).
- Dates must be ISO "YYYY-MM-DD" or the literal "N/A".
- Eligibility is a concise canonical label (see prompt) with optional suffixes.
- Application Method is one of: Online, Paper, Other.
- If a comment isn’t a timeline, we skip it.

Usage:
  OPENAI_API_KEY=... python extract_timelines.py input.json output.tsv --model gpt-4o-mini

Notes:
- Keeps things simple by using Chat Completions with response_format=json_object.
- Writes incrementally: opens the output in append mode, writes the header once (if empty),
  writes a single row immediately after each successful parse, and flushes per row.
  To start fresh, delete the output file before re-running.
"""

import argparse, json, os, sys, time
from typing import Any

# ---- Config ----
DEFAULT_MODEL = os.environ.get("OPENAI_MODEL", "gpt-5")
RATE_LIMIT_DELAY_SEC = float(os.environ.get("RATE_LIMIT_DELAY_SEC", "0.3"))  # gentle pacing

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
    "Eligibility",
    "Application Method",
    "Application Date",
    "Biometric Date",
    "Approval Date",
    "Ceremony Date",
])

def _canonical_eligibility(model_value: str, body: str) -> str:
    """
    Pick the best eligibility base + suffixes from both the model's output and raw body text.
    Priority bases (checked by keywords):
      EUSS, MN1 (Child), Form T, BNO, Armed Forces, ILR (default)
    Optional suffixes: (+ Marriage), (+ DV), (+ Refugee)
    """
    text = f"{model_value} || {body}".lower()

    # Base detection (priority order to avoid misclassifying children/BNO/etc. as ILR)
    if any(k in text for k in ["euss", "settled status", "eu settlement", "eu settled"]):
        base = "EUSS"
    elif any(k in text for k in ["mn1", "minor child", "child application", "registration of a minor"]):
        base = "MN1 (Child)"
    elif "form t" in text or ("born in the uk" in text and "10" in text and "year" in text):
        base = "Form T"
    elif any(k in text for k in ["bno", "british national (overseas)"]):
        base = "BNO"
    elif "armed forces" in text or "hm forces" in text:
        base = "Armed Forces"
    else:
        # Default catch-all: ILR (covers Tier 2/SWV/Ancestry/Refugee/GT → ILR narratives)
        base = "ILR"

    # Suffixes (can add more than one if clearly stated)
    suffixes = []
    if any(k in text for k in ["married to british", "british spouse", "spouse of a british", "uk spouse", "married to a british"]):
        suffixes.append(" (+ Marriage)")
    if any(k in text for k in ["ilrdv", "domestic violence"]):
        suffixes.append(" (+ DV)")
    if "refugee" in text:
        suffixes.append(" (+ Refugee)")

    return base + "".join(suffixes)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("input_json", help="Path to input JSON (with top-level 'comments' array).")
    parser.add_argument("output_tsv", help="Path to write the TSV.")
    parser.add_argument("--model", default=DEFAULT_MODEL, help=f"OpenAI model (default: {DEFAULT_MODEL})")
    args = parser.parse_args()

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

    # Lazy import to keep the script dependency-light.
    try:
        from openai import OpenAI
    except Exception as e:
        print("Please install the official OpenAI Python SDK: pip install openai", file=sys.stderr)
        sys.exit(1)

    client = OpenAI()

    # Prepare output file for incremental writes (append mode).
    rows_written = 0
    need_header = not os.path.exists(args.output_tsv) or os.path.getsize(args.output_tsv) == 0
    try:
        out_f = open(args.output_tsv, "a", encoding="utf-8", newline="")
    except Exception as e:
        print(f"Failed to open output TSV for appending: {e}", file=sys.stderr)
        sys.exit(1)

    try:
        if need_header:
            out_f.write(TSV_HEADER + "\n")
            out_f.flush()

        for idx, c in enumerate(comments, start=1):
            print(c)
            body = c.get("body", "")
            if not body or not isinstance(body, str):
                continue

            # Build the chat with strict JSON object response.
            try:
                resp = client.chat.completions.create(
                    model=args.model,
                    response_format={"type": "json_object"},
                    messages=[
                        {"role": "system", "content": SYSTEM_PROMPT},
                        {"role": "user", "content": USER_PROMPT_TEMPLATE.format(body=body)},
                    ],
                )
            except Exception as e:
                print(f"[warn] OpenAI call failed for comment #{idx}: {e}", file=sys.stderr)
                if RATE_LIMIT_DELAY_SEC:
                    time.sleep(RATE_LIMIT_DELAY_SEC)
                continue

            try:
                content = resp.choices[0].message.content
                parsed = json.loads(content)
            except Exception as e:
                print(f"[warn] Could not parse JSON for comment #{idx}: {e}", file=sys.stderr)
                try:
                    print(f"[warn] Model output was:\n{resp.choices[0].message.content}", file=sys.stderr)
                except Exception:
                    pass
                continue

            if parsed.get("skip") is True:
                if RATE_LIMIT_DELAY_SEC:
                    time.sleep(RATE_LIMIT_DELAY_SEC)
                continue

            # ---- Eligibility (enhanced, but still concise) ----
            eligibility = str(parsed.get("eligibility", "")).strip()
            eligibility_out = _canonical_eligibility(eligibility, body)

            def norm(x: Any) -> str:
                val = str(x or "").strip()
                # Enforce "YYYY-MM-DD" or "N/A" only (we rely on the model to do the heavy lifting).
                if val == "N/A":
                    return val
                return val if len(val) == 10 and val[4] == "-" and val[7] == "-" else "N/A"

            row = [
                eligibility_out,
                str(parsed.get("application_method", "Online")).strip().title(),
                norm(parsed.get("application_date")),
                norm(parsed.get("biometric_date")),
                norm(parsed.get("approval_date")),
                norm(parsed.get("ceremony_date")),
            ]

            # Write this row immediately and flush.
            try:
                out_f.write("\t".join(row) + "\n")
                out_f.flush()
                rows_written += 1
            except Exception as e:
                print(f"[warn] Failed to write a row for comment #{idx}: {e}", file=sys.stderr)

            if RATE_LIMIT_DELAY_SEC:
                time.sleep(RATE_LIMIT_DELAY_SEC)

    finally:
        try:
            out_f.close()
        except Exception:
            pass

    print(f"Wrote {rows_written} row(s) to {args.output_tsv}")

if __name__ == "__main__":
    main()
