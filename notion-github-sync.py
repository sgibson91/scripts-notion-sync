import os

import pandas as pd
from notion_client import Client
from tqdm import tqdm


def create_page_metadata(item):
    properties = {
        "Closed at": {"type": "date"},
        "Created at": {
            "type": "date",
            "date": {"start": item["created_at"].isoformat()},
        },
        "Filters": {"type": "multi_select"},
        "Number": {"type": "number", "number": item["number"]},
        "PR": {"type": "checkbox", "checkbox": item["pull_request"]},
        "Repository": {
            "type": "rich_text",
            "rich_text": [
                {
                    "text": {
                        "content": item["repo_name"],
                    },
                    "plain_text": item["repo_name"],
                    "href": item["repo_url"],
                }
            ],
        },
        "State": {
            "type": "select",
            "select": {"name": item["state"]},
        },
        "Title": {
            "title": [
                {
                    "text": {
                        "content": item["raw_title"],
                    },
                }
            ]
        },
        "Updated at": {
            "type": "date",
            "date": {"start": item["updated_at"].isoformat()},
        },
        "URL": {"type": "url", "url": item["link"]},
    }

    # Handle  the closed at property
    if not pd.isnull(item["closed_at"]):
        properties["Closed at"]["date"] = {"start": item["closed_at"].isoformat()}
    else:
        properties["Closed at"]["date"] = None

    # Handle the filter property
    filters_to_apply = [
        filter_name.replace("_", " ") for filter_name in set(item["filter"].split(":"))
    ]
    properties["Filters"]["multi_select"] = []

    # Populate filters
    for filter_name in filters_to_apply:
        properties["Filters"]["multi_select"].append({"name": filter_name})

    return properties


# Consume environment variables
notion_token = os.environ.get("NOTION_TOKEN", None)
notion_db_id = os.environ.get("NOTION_DATABASE_ID", None)

# Check env vars are set
for name, val in {
    "NOTION_TOKEN": notion_token,
    "NOTION_DATABASE_ID": notion_db_id,
}.items():
    if val is None:
        raise ValueError(f"{name} must be set!")

# Authenticate the Notion client
notion = Client(auth=notion_token)

# Consume the raw data from sister repo
data_url = "https://raw.githubusercontent.com/sgibson91/github-activity-dashboard/main/github-activity.csv"
df = pd.read_csv(
    data_url,
    parse_dates=["created_at", "updated_at", "closed_at"],
    infer_datetime_format=True,
)

for i, row in tqdm(df.iterrows(), total=len(df)):
    # Query the database
    results = notion.databases.query(
        **{
            "database_id": notion_db_id,
            "filter": {"property": "Title", "text": {"contains": row["raw_title"]}},
        }
    ).get("results")

    # Generate metadata for the item
    page_metadata = create_page_metadata(row)

    if results:
        # Update existing page
        notion.pages.update(results[0]["id"], properties=page_metadata)
    else:
        # Create new page
        notion.pages.create(
            parent={"database_id": notion_db_id}, properties=page_metadata
        )

# TODO: If a page exists in the Notion database, but NOT in the csv file, delete the page
