import os
import re

import pandas as pd
from dotenv import load_dotenv
from notion_client import Client
from rich import print
from tqdm import tqdm


def read_clippings_file(filepath):
    with open(filepath) as f:
        clippings = f.read()

    clippings = clippings.split("==========")
    clippings = clippings[:-1]

    return clippings


def process_clippings(clippings):
    highlights = {}

    for clipping in clippings:
        # Extract book title from clipping
        try:
            match = re.search(r".*(?<=\()", clipping)
            title = match.group(0).strip("(").strip()
        except AttributeError:
            title = clipping.split("\n")[0]

        # Extract author from clipping
        match = re.findall(r"\(([^\)]+)\)", clipping)
        if len(match) > 1:
            author = match[-1]
        elif len(match) == 1:
            author = match[0]
        else:
            author = ""

        # Extract the highlight from the clipping
        split_clipping = clipping.split("\n")
        split_clipping = [clip for clip in split_clipping if clip != ""]
        highlight = split_clipping[-1]

        if title not in highlights:
            highlights[title] = {
                "author": author,
                "highlights": [highlight],
            }
        else:
            highlights[title]["highlights"].append(highlight)

    # Remove any null entries
    highlights.pop("", None)

    return highlights


def create_page_metadata(title, metadata):
    page_metadata = {}
    page_metadata["icon"] = {
        "type": "emoji",
        "emoji": "🔖",
    }
    page_metadata["properties"] = {
        "Title": {
            "title": [
                {
                    "text": {
                        "content": title,
                    }
                }
            ]
        },
        "Author": {
            "type": "rich_text",
            "rich_text": [
                {
                    "type": "text",
                    "text": {
                        "content": metadata["author"],
                    },
                }
            ],
        },
    }
    page_metadata["children"] = [
        {
            "object": "block",
            "type": "heading_2",
            "heading_2": {
                "rich_text": [
                    {
                        "type": "text",
                        "text": {
                            "content": "✏️ Highlights",
                            "link": None,
                        },
                    }
                ],
                "color": "default",
                "is_toggleable": False,
            },
        }
    ]

    for highlight in metadata["highlights"]:
        page_metadata["children"].append(
            {
                "object": "block",
                "type": "quote",
                "quote": {
                    "rich_text": [
                        {
                            "type": "text",
                            "text": {
                                "content": highlight,
                                "link": None,
                            },
                        }
                    ],
                    "color": "default",
                },
            }
        )

    return page_metadata


load_dotenv()
NOTION_DATABASE_ID = os.getenv("NOTION_DATABASE_ID")
NOTION_TOKEN = os.getenv("NOTION_TOKEN")

notion = Client(auth=NOTION_TOKEN)

filepath = "/Users/sgibson/Google Drive/Other computers/USB and external devices/Kindle/documents/My Clippings.txt"
clippings = read_clippings_file(filepath)
highlights = process_clippings(clippings)
set_highlights = set(highlights.keys())

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
notion_pages = pd.DataFrame(notion_pages)
set_notion = set(notion_pages["title"].values)

# Difference - Titles which ARE in kindle highlights but ARE NOT in the
# Notion DB
to_be_created = set_highlights.difference(set_notion)
print("[green]Number of pages to be created:", len(to_be_created))

# Intersection - Titles which are in BOTH kindle highlights and Notion DB
to_be_updated = set_highlights.intersection(set_notion)
print("[green]Number of pages to be updated:", len(to_be_updated))

# Difference - Titles which ARE in the Notion DB but ARE NOT in kindle highlights
to_be_archived = set_notion.difference(set_notion)
print("[green]Number of pages to be archived:", len(to_be_archived))

if len(to_be_created) > 0:
    print("[green]Creating new pages...")
    for title in tqdm(to_be_created, total=len(to_be_created)):
        metadata = highlights[title]

        page_metadata = create_page_metadata(title, metadata)

        notion.pages.create(
            parent={"database_id": NOTION_DATABASE_ID},
            icon=page_metadata["icon"],
            properties=page_metadata["properties"],
            children=page_metadata["children"],
        )

if len(to_be_updated) > 0:
    print("[green]Updating existing pages...")
    extra_pages_to_archive = []
    for title in tqdm(to_be_updated, total=len(to_be_updated)):
        # Find the page ID
        page_id = notion_pages["page_id"].loc[notion_pages["title"] == title]

        if len(page_id) > 1:
            # Append extra IDs to list to archive later
            extra_pages_to_archive.extend(page_id[1:])
        page_id = page_id.values[0]

        page_metadata = create_page_metadata(title, highlights[title])

        # Update the page
        notion.pages.update(
            page_id,
            properties=page_metadata["properties"],
            children=page_metadata["children"],
        )

if len(to_be_archived) > 0:
    print("[green]Archiving old pages...")
    for title in tqdm(to_be_archived, total=len(to_be_archived)):
        # Find the pages IDs - could be multiple
        page_ids = notion_pages[notion_pages["title"] == title]["page_id"].values

        for page_id in page_ids:
            # Archive the page
            notion.pages.update(page_id, archived=True)

if len(extra_pages_to_archive) > 0:
    print("[green]Archiving duplicated pages...")
    for page_id in extra_pages_to_archive:
        notion.pages.update(page_id, archived=True)

print("[green]Sync complete!")
