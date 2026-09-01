-- Schema for the metadata database. Idempotent: safe to apply repeatedly.

CREATE TABLE IF NOT EXISTS data_pipelines (
    pipeline_name      text PRIMARY KEY,
    schedule           text NOT NULL,          -- CRON expression
    is_schedule_paused boolean NOT NULL DEFAULT false
);

INSERT INTO data_pipelines (pipeline_name, schedule)
VALUES ('sample_pipeline', '*/5 * * * *')
ON CONFLICT (pipeline_name) DO NOTHING;

-- ── Storage layer ────────────────────────────────────────────────────────────
-- A warehouse is a catalog+storage pairing on some platform. Entities (datasets,
-- filesets) are logical; where they physically live is an entity_placement row.

CREATE TABLE IF NOT EXISTS warehouses (
    warehouse_name text PRIMARY KEY,      -- 'local-seaweedfs', later 'aws-glue', 'gcs-biglake'...
    platform       text NOT NULL,         -- 'local' | 'aws' | 'gcp'... resolution key
    catalog_kind   text NOT NULL,         -- 'sql' | 'glue' | 'rest' | 's3tables'...
    catalog_config jsonb NOT NULL,
    storage_kind   text NOT NULL,         -- 's3' | 'gcs' | 'file'...
    storage_config jsonb NOT NULL,        -- endpoint, bucket, creds (local-dev plaintext)
    connector      text NOT NULL DEFAULT 'pyiceberg'  -- SDK handler hint
);

-- Logical registry for datasets AND filesets. version bumps on backwards-incompatible schema.
CREATE TABLE IF NOT EXISTS entities (
    entity_id    uuid PRIMARY KEY,
    entity_type  text NOT NULL,           -- 'dataset' | 'fileset'
    namespace    text NOT NULL,
    name         text NOT NULL,
    version      int  NOT NULL DEFAULT 1,
    partition_by jsonb,                   -- datasets only; may be [] (explicit unpartitioned)
    description  text,
    created_by   text NOT NULL,
    created_at   timestamptz NOT NULL DEFAULT now(),
    dropped_at   timestamptz,             -- soft delete: lineage history survives
    UNIQUE (namespace, name, version)
);

-- Where an entity version physically lives; one row per warehouse it's placed in.
CREATE TABLE IF NOT EXISTS entity_placements (
    entity_id           uuid NOT NULL REFERENCES entities (entity_id),
    warehouse_name      text NOT NULL REFERENCES warehouses,
    physical_identifier text NOT NULL,    -- table name in that warehouse ('ns.name', '_v{n}' for versions >1)
    format              text NOT NULL DEFAULT 'iceberg',
    created_at          timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (entity_id, warehouse_name)
);

-- One immutable ledger for ALL operations: writes, reads, alters, rollbacks, drops.
CREATE TABLE IF NOT EXISTS ops (
    op_id          uuid PRIMARY KEY,
    entity_id      uuid NOT NULL REFERENCES entities (entity_id),
    warehouse_name text,                  -- which placement the op hit
    op_kind        text NOT NULL,         -- create|append|overwrite|alter|rollback|drop|read
    actor          text NOT NULL,
    process_kind   text,                  -- 'pipeline' | 'model_run' | 'notebook' | 'adhoc'...
    process_name   text,
    process_run_id text,
    snapshot_id    bigint,                -- resolution cache; durable link = op_id in snapshot summary
    details        jsonb NOT NULL DEFAULT '{}',
    occurred_at    timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ops_entity_time ON ops (entity_id, occurred_at DESC);
-- ponytail: reads and writes share this table; split reads out if their volume ever swamps it.

INSERT INTO warehouses (warehouse_name, platform, catalog_kind, catalog_config, storage_kind, storage_config)
VALUES
    ('local-seaweedfs', 'local', 'sql',
     '{"uri": "postgresql+psycopg2://lakehouse_pg_user:lakehouse_pg_password@localhost:5433/metadata", "warehouse": "s3://lakehouse/warehouse"}',
     's3',
     '{"endpoint_url": "http://localhost:8333", "bucket": "lakehouse", "access_key": "lakehouse", "secret_key": "lakehouse_secret"}'),
    ('local-rustfs', 'local', 'sql',
     '{"uri": "postgresql+psycopg2://lakehouse_pg_user:lakehouse_pg_password@localhost:5433/metadata", "warehouse": "s3://lakehouse/warehouse"}',
     's3',
     '{"endpoint_url": "http://localhost:9010", "bucket": "lakehouse", "access_key": "lakehouse", "secret_key": "lakehouse_secret"}')
ON CONFLICT (warehouse_name) DO NOTHING;
