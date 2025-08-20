import json
import os
from pathlib import Path

import click
import psycopg2
from psycopg2.extras import Json, execute_values
from sshtunnel import SSHTunnelForwarder
from tqdm import tqdm

DB_NAME = os.getenv("DB_NAME")
DB_USERNAME = os.getenv("DB_USERNAME")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_PORT = os.getenv("DB_PORT")
DB_HOST = os.getenv("DB_HOST")
SSH_CONFIG_NAME = os.getenv("SSH_CONFIG_NAME")
SSH_USERNAME = os.getenv("SSH_USERNAME")
SSH_KEY_PATH = os.getenv("SSH_KEY_PATH")
SSH_REMOTE_BIND_ADDRESS = os.getenv("SSH_REMOTE_BIND_ADDRESS")
SSH_REMOTE_BIND_PORT = int(os.getenv("SSH_REMOTE_BIND_PORT", 5432))
USE_TUNNEL = True if os.getenv("USE_TUNNEL").lower() in ["true", "yes", "1"] else False


@click.command()
@click.argument(
    "in_directory",
    type=click.Path(
        exists=True,
        file_okay=False,
    ),
)
def load_json_to_postgres(in_directory):
    # Database connection
    print(
        f"Attempting to connect to {DB_HOST}:{DB_PORT} and Database {DB_NAME} as user {DB_USERNAME}"
    )

    if USE_TUNNEL:
        tunnel = SSHTunnelForwarder(
            "wqa-tunnel",  # SSH connection to your VM
            ssh_username=SSH_USERNAME,
            ssh_pkey=SSH_KEY_PATH,  # or ssh_pkey for key auth
            remote_bind_address=(SSH_REMOTE_BIND_ADDRESS, SSH_REMOTE_BIND_PORT),
            local_bind_address=(
                DB_HOST,
                DB_PORT,
            ),
        )
        tunnel.start()
        conn = psycopg2.connect(
            host=DB_HOST,
            port=tunnel.local_bind_port,
            database=DB_NAME,
            user=DB_USERNAME,
            password=DB_PASSWORD,
        )
    else:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USERNAME,
            password=DB_PASSWORD,
        )
    conn.autocommit = True
    cursor = conn.cursor()

    directory = Path(in_directory)
    files = list(directory.glob("*.json"))

    for filename in tqdm(files, desc="Processing files", unit="file"):
        file_path = os.path.join(in_directory, filename.name)
        try:
            _ = int(filename.name.replace(".json", "", 1))
            public = True
        except ValueError:
            public = False

        # Read and process each JSON file
        with open(file_path, "r") as f:
            all_data = json.load(f)

        if not isinstance(all_data, list):
            all_data = [all_data]

        batch_data = []
        for data in tqdm(
            all_data,
            desc=f"Processing records in {filename.name}",
            unit="record",
            leave=False,
        ):
            # Extract data for database insertion
            immutable_id = data.get("id")
            inserted_at_revision = data.get("insertedAtRevision") or 0
            deleted_at_revision = data.get("deletedAtRevision")
            edit = None
            content = Json(data)  # leave it as raw json dicts
            if "type" in data:
                data_type = (
                    "Attribute"
                    if data.get("type").endswith("Attribute")
                    else data.get("type")
                )
            else:
                if data.get("format"):
                    data_type = "FormatType"

            batch_data.append(
                (
                    immutable_id,
                    public,
                    data_type,
                    inserted_at_revision,
                    deleted_at_revision,
                    edit,
                    content,
                )
            )

        if batch_data:
            # Insert data into the database

            insert_query = """
                    INSERT INTO assets (immutable_id, public, type, inserted_at_revision, deleted_at_revision, edit, content)
                    VALUES %s
                    ON CONFLICT (immutable_id, inserted_at_revision) DO UPDATE SET
                    public = EXCLUDED.public,
                    type = EXCLUDED.type,
                    deleted_at_revision = EXCLUDED.deleted_at_revision,
                    edit = EXCLUDED.edit,
                    content = EXCLUDED.content;
                """

            try:
                tqdm.write(f"Inserting {len(batch_data)} records from {filename.name}")
                execute_values(
                    cursor,
                    insert_query,
                    batch_data,
                    template=None,
                    page_size=4000,
                )

                tqdm.write(
                    f"Batch inserted {len(batch_data)} records from {filename.name}"
                )
            except psycopg2.DatabaseError as e:
                pass
                tqdm.write(f"Skipping {immutable_id} update: {e}")


# Usage
if __name__ == "__main__":
    load_json_to_postgres()
