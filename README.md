# postgress-ingester
A repository providing a postgres DB upsert script in Python.

### How to use this repo
We provide a docker-compose file to spin up a local postgres database server which you can use to test the ingester. On startup the `init.sql` script will run which creates a new `assets` table as well as a new example user `test_user` with password `password`, which is then used by the ingester to upsert data into the database. To start the docker container run `docker compose up` and check the logs to ensure the database and user has been created.
Next, we can run the ingester. We provide two ingester examples in this repository, the first, `load-from-json`, interacts with the database directly by executing SQL commands as text, for example
```
INSERT INTO assets (immutable_id, public, type, inserted_at_revision, deleted_at_revision, edit, content)
VALUES %s
ON CONFLICT (immutable_id, inserted_at_revision) DO UPDATE SET
public = EXCLUDED.public,
type = EXCLUDED.type,
deleted_at_revision = EXCLUDED.deleted_at_revision,
edit = EXCLUDED.edit,
content = EXCLUDED.content;
```

The second script, `load-from-json-sqlalchemy`, interacts with the database using the SQLAlchemy Object Relational Mapper (ORM), which allows SQL interaction to be done using Python classes instead of SQl commands, for example
```
class Assets(SQLModel, table=True):
    immutable_id: str = Field(primary_key=True)
    inserted_at_revision: int = Field(primary_key=True)
    urn: str
    public: bool
    type: str
    deleted_at_revision: int | None = None
    edit: dict | None = Field(default=None, sa_column=Column(JSON))
    content: dict | None = Field(default=None, sa_column=Column(JSON))

## Command to insert some new data into the "assets" table
stmt = insert(Assets).values(**asset_data)
```

Both of the provided scripts use the Click Python package to enable them to be executed as Command Line Interfaces (CLIs). You can call them as below, making sure to provide the file path to a directory containing your JSON files to be ingested:
```
python ./load-from-json.py ./path/to/json-files
```

Once running, you will see logging updates on the progress being made, including any warnings or errors that have occurred during import.

Once completed, we suggest you make use of some postgres client, such as pgAdmin, or `psql` on the command line, to check your database state after the ingest.