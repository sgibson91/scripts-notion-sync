import os

import pandas as pd
from notion_client import Client
from tqdm import tqdm


def create_page_metadata(item):
    properties = {
        "Filters": {"type": "multi_select"},
        "PR": {
            "type": "checkbox",
            "checkbox": bool(item["pull_request"]),
        },
        "Repository URL": {"type": "url", "url": item["repo_url"]},
        "Title": {
            "title": [
                {
                    "text": {
                        "content": item["raw_title"],
                    },
                }
            ]
        },
        "URL": {"type": "url", "url": item["link"]},
    }

    # Handle the `filter` property
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
)

# Filter for items that are 'review_requested' or 'assigned'
df = df[df["filter"].str.contains("assigned|review_requested")]

# Filter for items that are open
df = df[df["state"] == "open"]

# Create a set of unique issue titles from the CSV
df_set = set(df["raw_title"].values)

# Create an empty DataFrame to store the Notion db in
notion_db = pd.DataFrame(columns=["page_id", "title", "archived"])
notion_db["archived"] = notion_db["archived"].astype("bool")

# First iteration - querying the Notion database for pages
# No filter will return all pages
resp = notion.databases.query(notion_db_id)
results = resp.get("results")

# Append each page to the DataFrame
for page in results:
    tmp_df = pd.DataFrame(
        {
            "page_id": page["id"],
            "title": page["properties"]["Title"]["title"][0]["plain_text"],
            "archived": page["archived"],
        },
        index=[0],
    )
    notion_db = pd.concat([notion_db, tmp_df], ignore_index=True)

# Pagination!
# has_more variable is boolean, is True if there are more pages to process
# next_cursor variable contains the position to pick-up querying from, it is only present
# if has_more is True
while resp["has_more"]:
    resp = notion.databases.query(notion_db_id, start_cursor=resp["next_cursor"])
    results = resp.get("results")

    for page in results:
        tmp_df = pd.DataFrame(
            {
                "page_id": page["id"],
                "title": page["properties"]["Title"]["title"][0]["plain_text"],
                "archived": page["archived"],
            },
            index=[0],
        )
        notion_db = pd.concat([notion_db, tmp_df], ignore_index=True)

notion_db.reset_index(inplace=True, drop=True)

# Create a set of unique titles from the Notion db
notion_db_set = set(notion_db["title"].values)

# Intersection - Titles which are in BOTH the CSV and Notion db
to_be_updated = df_set.intersection(notion_db_set)
print("Number of pages to update:", len(to_be_updated))

# Difference - Titles which ARE in the CSV but ARE NOT in the Notion db
to_be_created = df_set.difference(notion_db_set)
print("Number of pages to create:", len(to_be_created))

# Difference - Titles which ARE in the Notion db but ARE NOT in the CSV
to_be_archived = notion_db_set.difference(df_set)
print("Number of pages to archive:", len(to_be_archived))

if len(to_be_updated) > 0:
    print("Updating existing pages...")
    extra_pages_to_archive = []
    for title in tqdm(to_be_updated, total=len(to_be_updated)):
        # Find the page ID
        page_id = notion_db["page_id"].loc[notion_db["title"] == title].values

        if len(page_id) > 1:
            # Append extra page IDs to list to archive later
            extra_pages_to_archive.extend(page_id[1:])

        page_id = page_id[0]

        # Find the corresponding row in the CSV dataframe
        row = df[df["raw_title"] == title].iloc[0]

        # Generate page metadata
        page_metadata = create_page_metadata(row)

        # Update the page
        notion.pages.update(page_id, properties=page_metadata)

if len(to_be_created) > 0:
    print("Creating new pages...")
    for title in tqdm(to_be_created, total=len(to_be_created)):
        # Find the corresponding row in the CSV dataframe
        row = df[df["raw_title"] == title].iloc[0]

        # Generate page metadata
        page_metadata = create_page_metadata(row)

        # Create the page
        notion.pages.create(
            parent={"database_id": notion_db_id}, properties=page_metadata
        )

if len(to_be_archived) > 0:
    print("Archiving old pages...")
    for title in tqdm(to_be_archived, total=len(to_be_archived)):
        # Find the page IDs - could be multiple
        page_ids = notion_db[notion_db["title"] == title]["page_id"].values

        for page_id in page_ids:
            # Archive the page
            notion.pages.update(page_id, archived=True)

if len(extra_pages_to_archive) > 0:
    print("Archiving duplicated pages...")
    for page_id in extra_pages_to_archive:
        notion.pages.update(page_id, archived=True)

print("Sync complete!")
