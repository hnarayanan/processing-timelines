import requests
import json
import re
import time

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
        headers = {'User-Agent': 'MyCorrectedToplevelDownloader/5.0'}

        print(f"    -> Fetching data for a batch of {len(chunk)} comment IDs...")
        time.sleep(1) # Be respectful to the API

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
    """
    comments = []
    for item in comment_children:
        if item.get('kind') != 't1':
            continue

        data = item['data']

        # If the comment's parent is not the main post, it's a nested reply. Skip it.
        if data.get('parent_id') != post_id:
            continue

        comment = {
            'comment_id': data.get('name'),
            'author': data.get('author', '[deleted]'),
            'body': data.get('body', ''),
            'score': data.get('score', 0),
        }
        comments.append(comment)

    return comments

def fetch_reddit_thread_all_toplevel_correct(thread_url):
    """
    Fetches a Reddit thread and ALL of its top-level comments, correctly
    filtering out any nested replies returned by the API.
    """
    if not thread_url.endswith('/'):
        thread_url += '/'

    json_url = thread_url + '.json'
    headers = {'User-Agent': 'MyCorrectedToplevelDownloader/5.0'}

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

        thread = {
            'post': {'title': post_data.get('title'), 'author': post_data.get('author'), 'selftext': post_data.get('selftext')},
            'comments': top_level_comments
        }

        return thread

    except requests.exceptions.RequestException as e:
        print(f"❌ An error occurred: {e}")
        return None

if __name__ == "__main__":
    url = "https://www.reddit.com/r/ukvisa/comments/1hkp9zl/naturalisation_citizenship_application_processing/"

    reddit_thread = fetch_reddit_thread_all_toplevel_correct(url)

    if reddit_thread:
        filename = "processing_timelines_raw_data.json"

        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(reddit_thread, f, ensure_ascii=False, indent=4)

        print(f"\n✅ Success! All {len(reddit_thread['comments'])} top-level comments correctly saved to '{filename}'")
