#!/usr/bin/env python3
"""
Fetch Reddit thread comments with edit tracking.
Captures edit timestamps to help track comment updates.
"""

import requests
import json
import time
from datetime import datetime


def _fetch_remaining_comments_data(post_id, children_ids):
    """
    Fetches raw comment data from a 'more' object.
    It chunks requests to stay within the API's limits (100 IDs per request).
    """
    all_comments_data = []
    # Process the IDs in chunks of 100
    for i in range(0, len(children_ids), 100):
        chunk = children_ids[i:i+100]
        ids_string = ",".join(chunk)

        url = "https://www.reddit.com/api/morechildren.json"
        params = {"api_type": "json", "link_id": post_id, "children": ids_string}
        headers = {'User-Agent': 'UKNaturalisationTimelineTracker/2.0'}

        print(f"    -> Fetching data for a batch of {len(chunk)} comment IDs...")
        time.sleep(1)  # Be respectful to the API

        try:
            response = requests.get(url, params=params, headers=headers)
            response.raise_for_status()
            new_data = response.json().get("json", {}).get("data", {}).get("things", [])
            all_comments_data.extend(new_data)
        except requests.exceptions.RequestException as e:
            print(f"    -> ❌ Could not fetch a batch of comments: {e}")
            continue

    return all_comments_data


def filter_and_parse_toplevel_comments(comment_children, post_id):
    """
    Parses a list of comment data objects, KEEPING ONLY true top-level comments.
    It does this by checking if the comment's 'parent_id' matches the post's ID.
    Also captures edit timestamps and created timestamps.
    """
    comments = []
    for item in comment_children:
        if item.get('kind') != 't1':
            continue

        data = item['data']

        # If the comment's parent is not the main post, it's a nested reply. Skip it.
        if data.get('parent_id') != post_id:
            continue

        # Extract timestamps
        created_utc = data.get('created_utc', 0)
        edited = data.get('edited', False)

        # Convert timestamps to ISO format for readability
        created_iso = datetime.fromtimestamp(created_utc).isoformat() if created_utc else None
        edited_iso = None
        if edited and isinstance(edited, (int, float)):
            edited_iso = datetime.fromtimestamp(edited).isoformat()

        comment = {
            'comment_id': data.get('name'),
            'author': data.get('author', '[deleted]'),
            'body': data.get('body', ''),
            'score': data.get('score', 0),
            'created_utc': created_utc,
            'created_iso': created_iso,
            'edited_utc': edited if isinstance(edited, (int, float)) else None,
            'edited_iso': edited_iso,
            'was_edited': bool(edited),
        }
        comments.append(comment)

    return comments


def fetch_reddit_thread_all_toplevel(thread_url):
    """
    Fetches a Reddit thread and ALL of its top-level comments, correctly
    filtering out any nested replies returned by the API.
    Includes edit tracking information.
    """
    if not thread_url.endswith('/'):
        thread_url += '/'

    json_url = thread_url + '.json'
    headers = {'User-Agent': 'UKNaturalisationTimelineTracker/2.0'}

    print(f"📡 Fetching initial data from: {json_url}")

    try:
        response = requests.get(json_url, headers=headers)
        response.raise_for_status()
        data = response.json()

        post_data = data[0]['data']['children'][0]['data']
        post_id = post_data.get('name')

        initial_comment_children = data[1]['data']['children']

        # Filter and parse the first batch of comments
        top_level_comments = filter_and_parse_toplevel_comments(initial_comment_children, post_id)

        # Find the 'more' object to get the IDs of remaining comments
        more_object = next((item for item in initial_comment_children if item.get('kind') == 'more'), None)

        if more_object:
            print("Found 'more' object, preparing to fetch and filter remaining comments...")
            remaining_ids = more_object['data'].get('children', [])

            if remaining_ids:
                # Fetch the raw data for all remaining comment IDs
                remaining_comments_data = _fetch_remaining_comments_data(post_id, remaining_ids)
                # Filter and parse the newly fetched data, adding only top-level comments
                newly_fetched_comments = filter_and_parse_toplevel_comments(remaining_comments_data, post_id)
                top_level_comments.extend(newly_fetched_comments)

        # Sort comments by creation time (oldest first)
        # top_level_comments.sort(key=lambda x: x.get('created_utc', 0))

        # Count edited comments
        edited_count = sum(1 for c in top_level_comments if c.get('was_edited'))

        thread = {
            'post': {
                'title': post_data.get('title'),
                'author': post_data.get('author'),
                'selftext': post_data.get('selftext'),
                'created_utc': post_data.get('created_utc'),
                'post_id': post_id,
            },
            'comments': top_level_comments,
            'metadata': {
                'total_comments': len(top_level_comments),
                'edited_comments': edited_count,
                'fetch_timestamp': datetime.now().isoformat(),
            }
        }

        return thread

    except requests.exceptions.RequestException as e:
        print(f"❌ An error occurred: {e}")
        return None


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="Fetch Reddit thread with all top-level comments and edit tracking"
    )
    parser.add_argument(
        "url",
        nargs="?",
        default="https://www.reddit.com/r/ukvisa/comments/1hkp9zl/naturalisation_citizenship_application_processing/",
        help="Reddit thread URL"
    )
    parser.add_argument(
        "-o", "--output",
        default="processing_timelines_raw_data.json",
        help="Output JSON file (default: processing_timelines_raw_data.json)"
    )
    args = parser.parse_args()

    reddit_thread = fetch_reddit_thread_all_toplevel(args.url)

    if reddit_thread:
        with open(args.output, 'w', encoding='utf-8') as f:
            json.dump(reddit_thread, f, ensure_ascii=False, indent=4)

        metadata = reddit_thread['metadata']
        print(f"\n✅ Success! Saved to '{args.output}'")
        print(f"   Total comments: {metadata['total_comments']}")
        print(f"   Edited comments: {metadata['edited_comments']}")
        print(f"   Fetch time: {metadata['fetch_timestamp']}")


if __name__ == "__main__":
    main()
