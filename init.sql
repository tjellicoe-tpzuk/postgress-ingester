CREATE USER test_user
with
    password 'password';

-- create a table
CREATE TABLE
    assets (
        immutable_id TEXT,
        "type" TEXT,
        inserted_at_revision INT NULL,
        deleted_at_revision INT NULL,
        edit jsonB NULL,
        content jsonB,
        public BOOLEAN,
        PRIMARY KEY (immutable_id, inserted_at_revision)
    );

CREATE INDEX idx_assets_immutable_inserted ON assets (immutable_id, inserted_at_revision);

CREATE INDEX idx_assets_immutable_deleted ON assets (immutable_id, deleted_at_revision);

-- Grant table privileges
GRANT ALL PRIVILEGES ON TABLE assets TO test_user;