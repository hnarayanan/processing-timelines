# UK Naturalisation Processing Times

A data pipeline that tracks UK citizenship (naturalisation)
application processing times from community-reported timelines on
Reddit.

If you find this useful, you can [explore the live
analysis][gs-analysis] or contact me at mail@harishnarayanan.org.

## What This Does

Automatically fetches timeline data from an [r/ukvisa naturalisation
thread][reddit-data], extracts structured information using OpenAI's
API, and tracks updates as applications progress. The processed data
powers [this analysis spreadsheet][gs-analysis].

## Installation

Install dependencies:
```bash
pip install openai requests
```

Set up your OpenAI API key:
```bash
export OPENAI_API_KEY="your-api-key"
```

## Usage

Fetch the latest Reddit data and extract timelines:
```bash
# Fetch raw data from Reddit
python fetch_thread.py \
  https://www.reddit.com/r/ukvisa/comments/1hkp9zl/ \
  -o raw_data.json

# Extract structured timeline data
python extract_timelines.py \
  raw_data.json \
  processing_timelines.tsv \
  --model gpt-5
```

The script intelligently handles:
- New applications
- Comment edits (detects and merges updates)
- Deleted comments (preserves historical data)
- Non-timeline comments (caches to avoid re-processing)

### Regular Updates

Run periodically to capture new data and updates:
```bash
python fetch_thread.py [...] -o raw_data.json
python extract_timelines.py raw_data.json processing_timelines.tsv
```

The system only processes new or changed comments, saving API costs.

### Manual Corrections

After reviewing the data, merge manual corrections:
```bash
python merge_manual_edits.py
```

## Configuration

Optional environment variables:
```bash
export OPENAI_MODEL="gpt-4o-mini"     # Default: gpt-5
export RATE_LIMIT_DELAY_SEC="0.1"     # Default: 0.1
```

## Authors and Contributing

This project is written and maintained by [Harish Narayanan](https://harishnarayanan.org).

If you're interested in contributing, please consider:
- Improving eligibility type detection
- Adding council-level analysis
- Enhancing data validation
- Reporting issues or submitting pull requests

## Copyright and License

Copyright (c) 2025 [Harish Narayanan](https://harishnarayanan.org).

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.

[gs-analysis]: https://docs.google.com/spreadsheets/d/1yvoZWplzg35EpXbjDP16V2UUSJekZeHwdUwvNsA-sIw/
[reddit-data]: https://www.reddit.com/r/ukvisa/comments/1hkp9zl/naturalisation_citizenship_application_processing/
