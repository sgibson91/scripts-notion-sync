import json
import os
import re
from datetime import datetime
from pathlib import Path

import feedparser
import jinja2
import pandas as pd
from notion_client import Client
from rich.console import Console
from rich.progress import track

console = Console(force_terminal=True)

# Get the path to the folder this script is in
PATH = Path(__file__).parent

# Read in the template JSON file with jinja
with open(PATH.joinpath("notion_page_book_template.json")) as f:
    template = jinja2.Template(f.read())

# Read in Goodreads shelves to query
with open(PATH.joinpath("shelves.txt")) as f:
    shelves = [line.strip("\n") for line in f.readlines()]


def clean_book_description(description):
    # Remove any html tags from the book description
    description = re.sub(r"(<[^>]*>)", "", description)

    # Ensure any double quotes and newlines are properly escaped or removed
    description = description.replace('\"', '\\"').replace("\n", "")

    return description


def extract_series_from_title(title):
    match = re.search(r"(?<=\().*?(?=#)", title)
    if match is not None:
        return match.group(0).replace(",", "").strip()
    else:
        return ""


def create_page_metadata(entry, shelf):
    # We have shelves that are `read-2` or `to-read-3` and we want to remove the
    # numbering
    if shelf.startswith("read-"):
        shelf = "read"
    elif shelf.startswith("to-read-"):
        shelf = "to-read"

    book_description = clean_book_description(entry.book_description)

    # Create a mapping of template variables for jinja template
    metadata_vars = {
        "author_name": entry.author_name,
        "book_description": book_description[:2000],
        "book_id": entry.book_id,
        "book_title": entry.title.replace('"', "'"),
        "cover_url": entry.book_large_image_url,
        "rating_num": int(entry.user_rating) if int(entry.user_rating) > 0 else None,
        "series": extract_series_from_title(entry.title),
        "shelf": shelf,
    }
    page_metadata = template.render(**metadata_vars)

    try:
        # Try to parse the metadata into a dictionary
        page_metadata = json.loads(page_metadata)

    except json.decoder.JSONDecodeError as err:
        # Create the errors folder if it doesn't exist
        if not os.path.exists(PATH.joinpath("errors")):
            os.mkdir(PATH.joinpath("errors"))

        # Create a file to save the faulty json to
        fn = re.sub(r"[^\w]", "_", entry.title) + ".json"
        with open(PATH.joinpath("errors", fn), "w") as f:
            f.write(page_metadata)

        raise err

    # Add extra variables to the template that require more complex logic
    page_metadata["properties"]["Fiction?"]["checkbox"] = (
        "non-fiction" not in entry.user_shelves
    )
    page_metadata["properties"]["Rating"]["number"] = (
        int(entry.user_rating) if int(entry.user_rating) > 0 else None
    )

    if shelf == "currently-reading":
        date_started = datetime.strptime(
            entry.user_date_added, "%a, %d %b %Y %H:%M:%S %z"
        )
        page_metadata["properties"]["Date last started"] = {
            "date": {"start": date_started.strftime("%Y-%m-%d")}
        }

    if shelf.startswith("read"):
        try:
            date_read_at = datetime.strptime(
                entry.user_read_at, "%a, %d %b %Y %H:%M:%S %z"
            )
            page_metadata["properties"]["Date last read"] = {
                "date": {"start": date_read_at.strftime("%Y-%m-%d")}
            }
        except ValueError:
            pass

    # If the book description is longer than 2000 characters, the upload to
    # Notion will fail. So we chunk up the description into multiple objects
    # of length 2000 characters.
    if len(book_description) > 2000:
        i_total = len(book_description) // 2000
        for i in range(1, i_total + 1):
            next_block = book_description[2000 * i: 2000 * (i + 1)]
            page_metadata["children"].append(
                {
                    "object": "block",
                    "type": "paragraph",
                    "paragraph": {
                        "rich_text": [
                            {
                                "type": "text",
                                "text": {"content": next_block, "link": None},
                            }
                        ],
                        "color": "default",
                    },
                }
            )

    return page_metadata


CI = os.getenv("CI", False)
if not CI:
    from dotenv import load_dotenv

    # Load in .env file
    load_dotenv()

GOODREADS_RSS_KEY = os.getenv("GOODREADS_RSS_KEY")
NOTION_DATABASE_ID = os.getenv("NOTION_DATABASE_ID")
NOTION_TOKEN = os.getenv("NOTION_TOKEN")

# Construct RSS base URL
rss_base_url = f"https://www.goodreads.com/review/list_rss/122919504?key={GOODREADS_RSS_KEY}&shelf="

# Authenticate the notion client
notion = Client(auth=NOTION_TOKEN)

# Retrieve all the books in the Goodreads RSS feeds for each shelf
goodreads_books = []
for shelf in shelves:
    feed = feedparser.parse(rss_base_url + shelf)

    for entry in feed.entries:
        try:
            page_metadata = create_page_metadata(entry, shelf)
            goodreads_books.append(
                {"title": entry.title, "page_metadata": page_metadata}
            )
        except json.decoder.JSONDecodeError as err:
            console.print(f"[red]Skipping {entry.title}")
            console.print(err)
            continue

# Convert into a dataframe
goodreads_books = pd.DataFrame(goodreads_books)
goodreads_books_title_set = set(goodreads_books["title"].values)

# First iteration - querying the notion database for pages, return all pages
resp = notion.databases.query(NOTION_DATABASE_ID)
results = resp.get("results")

notion_pages = []
for page in results:
    notion_pages.append(
        {
            "page_id": page["id"],
            "title": page["properties"]["Title"]["title"][0]["plain_text"],
            "archived": page["archived"],
        }
    )

# Pagination!
# has_more variable is boolean, is True if there are more pages to process
# next_cursor variable contains the postion to pick-up querying from, it is only
# present if has_more is True
while resp["has_more"]:
    resp = notion.databases.query(NOTION_DATABASE_ID, start_cursor=resp["next_cursor"])
    results = resp.get("results")

    for page in results:
        notion_pages.append(
            {
                "page_id": page["id"],
                "title": page["properties"]["Title"]["title"][0]["plain_text"],
                "archived": page["archived"],
            }
        )

# Convert into a dataframe
notion_pages = pd.DataFrame(notion_pages)
notion_pages["archived"] = notion_pages["archived"].astype(bool)
notion_pages_title_set = set(notion_pages["title"].values)

# Intersection - Titles which are in BOTH Goodreads RSS feed and Notion DB
to_be_updated = goodreads_books_title_set.intersection(notion_pages_title_set)
console.print("[green]Number of pages to be updated:", len(to_be_updated))

# Difference - Titles which ARE in the Goodreads RSS feed but ARE NOT in the
# Notion DB
to_be_created = goodreads_books_title_set.difference(notion_pages_title_set)
console.print("[green]Number of pages to be created:", len(to_be_created))

# Difference - Titles which ARE in the Notion DB but ARE NOT in the Goodreads RSS feed
to_be_archived = notion_pages_title_set.difference(goodreads_books_title_set)
console.print("[green]Number of pages to be archived:", len(to_be_archived))

if len(to_be_created) > 0:
    console.print("[green]Creating new pages...")
    for title in track(to_be_created):
        # Find the corresponding row in the Goodreads df
        row = goodreads_books[goodreads_books["title"] == title].iloc[0]

        notion.pages.create(
            parent={"database_id": NOTION_DATABASE_ID},
            icon=row["page_metadata"]["icon"],
            properties=row["page_metadata"]["properties"],
            children=row["page_metadata"]["children"],
        )

if len(to_be_updated) > 0:
    console.print("[green]Updating existing pages...")
    extra_pages_to_archive = []
    for title in track(to_be_updated):
        # Find the page ID
        page_id = notion_pages["page_id"].loc[notion_pages["title"] == title]

        if len(page_id) > 1:
            # Append extra IDs to list to archive later
            extra_pages_to_archive.extend(page_id[1:])

        page_id = page_id.values[0]

        # Find the corresponding row in the Goodreads df
        row = goodreads_books[goodreads_books["title"] == title].iloc[0]

        # Update the page
        notion.pages.update(
            page_id,
            properties=row["page_metadata"]["properties"],
            children=row["page_metadata"]["children"],
        )

if len(to_be_archived) > 0:
    console.print("[green]Archiving old pages...")
    for title in track(to_be_archived):
        # Find the pages IDs - could be multiple
        page_ids = notion_pages[notion_pages["title"] == title]["page_id"].values

        for page_id in page_ids:
            # Archive the page
            notion.pages.update(page_id, archived=True)

if len(extra_pages_to_archive) > 0:
    console.print("[green]Archiving duplicated pages...")
    for page_id in extra_pages_to_archive:
        notion.pages.update(page_id, archived=True)

console.print("[green]Sync complete!")
