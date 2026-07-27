-- Schema for the metadata database. Idempotent: safe to apply repeatedly.

CREATE TABLE IF NOT EXISTS data_pipelines (
    pipeline_name      text PRIMARY KEY,
    schedule           text NOT NULL,          -- CRON expression
    is_schedule_paused boolean NOT NULL DEFAULT false
);

INSERT INTO data_pipelines (pipeline_name, schedule)
VALUES ('sample_pipeline', '*/5 * * * *')
ON CONFLICT (pipeline_name) DO NOTHING;
