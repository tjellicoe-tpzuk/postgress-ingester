import asyncio
import json
import os
from pathlib import Path

import click
import psycopg2
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlmodel import JSON, Column, Field, SQLModel
from sshtunnel import SSHTunnelForwarder
from tqdm import tqdm

SSH_CONFIG_NAME = os.getenv("SSH_CONFIG_NAME")
SSH_USERNAME = os.getenv("SSH_USERNAME")
SSH_KEY_PATH = os.getenv("SSH_KEY_PATH")
SSH_REMOTE_BIND_ADDRESS = os.getenv("SSH_REMOTE_BIND_ADDRESS")
SSH_REMOTE_BIND_PORT = int(os.getenv("SSH_REMOTE_BIND_PORT", 5432))
USE_TUNNEL = True if os.getenv("USE_TUNNEL").lower() in ["true", "yes", "1"] else False

DB_URL = (
    f"postgresql://{os.getenv('DB_USERNAME')}:{os.getenv('DB_PASSWORD')}"
    f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
)

DB_ASYNC_URL = (
    f"postgresql+asyncpg://{os.getenv('DB_USERNAME')}:{os.getenv('DB_PASSWORD')}"
    f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
)


class Assets(SQLModel, table=True):
    immutable_id: str = Field(primary_key=True)
    inserted_at_revision: int = Field(primary_key=True)
    urn: str
    public: bool
    type: str
    deleted_at_revision: int | None = None
    edit: dict | None = Field(default=None, sa_column=Column(JSON))
    content: dict | None = Field(default=None, sa_column=Column(JSON))


async def upsert_asset(async_engine, asset_data):
    async with AsyncSession(async_engine) as session:
        stmt = insert(Assets).values(**asset_data)
        update_dict = asset_data.copy()
        # Remove keys you don't want to update (like primary keys)
        update_dict.pop("immutable_id", None)
        update_dict.pop("inserted_at_revision", None)
        stmt = stmt.on_conflict_do_update(
            index_elements=["immutable_id", "inserted_at_revision"], set_=update_dict
        )
        await session.execute(stmt)
        await session.commit()


async def load_json_to_postgres(in_directory):
    # Database connection
    if USE_TUNNEL:
        tunnel = SSHTunnelForwarder(
            SSH_CONFIG_NAME,  # SSH connection to your VM
            ssh_username=SSH_USERNAME,
            ssh_pkey=SSH_KEY_PATH,  # or ssh_pkey for key auth
            remote_bind_address=(SSH_REMOTE_BIND_ADDRESS, SSH_REMOTE_BIND_PORT),
            local_bind_address=(
                os.getenv("DB_HOST"),
                os.getenv("DB_PORT"),
            ),
        )
        tunnel.start()

    async_engine = create_async_engine(DB_ASYNC_URL, future=True)

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
            content = data  # leave it as raw json dicts
            if "type" in data:
                data_type = (
                    "Attribute"
                    if data.get("type").endswith("Attribute")
                    else data.get("type")
                )
            else:
                if data.get("format"):
                    data_type = "FormatType"

            update_dict = {
                "immutable_id": immutable_id,
                "public": public,
                "type": data_type,
                "inserted_at_revision": inserted_at_revision,
                "deleted_at_revision": deleted_at_revision,
                "edit": edit,
                "content": content,
            }

            batch_data.append(update_dict)

        if batch_data:
            try:
                tqdm.write(f"Inserting {len(batch_data)} records from {filename.name}")

                tasks = [upsert_asset(async_engine, data) for data in batch_data]

                await asyncio.gather(*tasks)

                tqdm.write(
                    f"Batch inserted {len(batch_data)} records from {filename.name}"
                )
            except psycopg2.DatabaseError as e:
                pass
                tqdm.write(f"Skipping {immutable_id} update: {e}")


@click.command()
@click.argument(
    "in_directory",
    type=click.Path(
        exists=True,
        file_okay=False,
    ),
)
def main(in_directory):
    asyncio.run(load_json_to_postgres(in_directory))


if __name__ == "__main__":
    main()
