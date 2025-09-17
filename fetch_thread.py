import requests
import json
import re

def parse_top_level_comments(comment_children):
    """
    Parses a list of comment objects from the Reddit API, but does NOT
    process their replies, grabbing only the top-level comments.
    """
    comments = []
    for comment_data in comment_children:
        # Skip "more comments" links and other non-comment types
        if comment_data.get('kind') != 't1':
            continue

        data = comment_data['data']
        comment = {
            'author': data.get('author', '[deleted]'),
            'body': data.get('body', ''),
            'score': data.get('score', 0),
        }
        # We intentionally do not process data['replies']
        comments.append(comment)

    return comments

def fetch_reddit_thread_toplevel(thread_url):
    """
    Fetches a Reddit thread and ONLY its top-level comments.
    This function makes only a single API request.

    Args:
        thread_url (str): The full URL of the Reddit thread.

    Returns:
        dict: A dictionary containing the post and a flat list of top-level
              comments, or None if the request fails.
    """
    if not thread_url.endswith('/'):
        thread_url += '/'

    json_url = thread_url + '.json'

    headers = {'User-Agent': 'MySimpleRedditDownloader/3.0'}

    print(f"📡 Fetching data in a single request from: {json_url}")

    try:
        response = requests.get(json_url, headers=headers)
        response.raise_for_status()

        data = response.json()

        post_data = data[0]['data']['children'][0]['data']
        comment_children = data[1]['data']['children']

        thread = {
            'post': {
                'title': post_data.get('title'),
                'author': post_data.get('author', '[deleted]'),
                'selftext': post_data.get('selftext'),
                'score': post_data.get('score'),
                'url': post_data.get('url')
            },
            'comments': parse_top_level_comments(comment_children)
        }

        return thread

    except requests.exceptions.RequestException as e:
        print(f"❌ An error occurred: {e}")
        return None
    except (IndexError, KeyError) as e:
        print(f"❌ Could not parse the JSON. Is this a valid thread URL? Error: {e}")
        return None

if __name__ == "__main__":
    url = "https://www.reddit.com/r/ukvisa/comments/1hkp9zl/naturalisation_citizenship_application_processing/"

    reddit_thread = fetch_reddit_thread_toplevel(url)

    if reddit_thread:
        post_title = reddit_thread['post']['title']
        safe_filename = re.sub(r'[^\w\s-]', '', post_title).strip().replace(' ', '_')
        filename = f"{safe_filename[:50]}_toplevel_only.json"

        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(reddit_thread, f, ensure_ascii=False, indent=4)

        print(f"\n✅ Success! Top-level thread saved to '{filename}'")
