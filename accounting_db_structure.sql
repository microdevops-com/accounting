
CREATE TABLE IF NOT EXISTS issues_checked (
	id SERIAL PRIMARY KEY,
	issue_id INTEGER NOT NULL,
	checked_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT now(),
	transaction_id BIGINT NOT NULL DEFAULT txid_current()
);

CREATE INDEX IF NOT EXISTS issues_checked_issue_id ON issues_checked (issue_id);
CREATE INDEX IF NOT EXISTS issues_checked_checked_at ON issues_checked (checked_at);
CREATE INDEX IF NOT EXISTS issues_checked_transaction_id ON issues_checked (transaction_id);


CREATE TABLE IF NOT EXISTS merge_requests_checked (
	id SERIAL PRIMARY KEY,
	merge_request_id INTEGER NOT NULL,
	checked_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT now(),
	transaction_id BIGINT NOT NULL DEFAULT txid_current()
);

CREATE INDEX IF NOT EXISTS merge_requests_checked_merge_request_id ON merge_requests_checked (merge_request_id);
CREATE INDEX IF NOT EXISTS merge_requests_checked_checked_at ON merge_requests_checked (checked_at);
CREATE INDEX IF NOT EXISTS merge_requests_checked_transaction_id ON merge_requests_checked (transaction_id);


CREATE TABLE IF NOT EXISTS hourly_employee_timelogs_checked (
	id SERIAL PRIMARY KEY,
	timelog_id INTEGER NOT NULL UNIQUE,
	checked_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT now(),
	transaction_id BIGINT NOT NULL DEFAULT txid_current()
);

CREATE INDEX IF NOT EXISTS hourly_employee_timelogs_checked_checked_at ON hourly_employee_timelogs_checked (checked_at);
CREATE INDEX IF NOT EXISTS hourly_employee_timelogs_checked_transaction_id ON hourly_employee_timelogs_checked (transaction_id);


CREATE TABLE IF NOT EXISTS hourly_timelogs_checked (
	id SERIAL PRIMARY KEY,
	timelog_id INTEGER NOT NULL UNIQUE,
	checked_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT now(),
	transaction_id BIGINT NOT NULL DEFAULT txid_current()
);

CREATE INDEX IF NOT EXISTS hourly_timelogs_checked_checked_at ON hourly_timelogs_checked (checked_at);
CREATE INDEX IF NOT EXISTS hourly_timelogs_checked_transaction_id ON hourly_timelogs_checked (transaction_id);


CREATE TABLE IF NOT EXISTS storage_usage (
	id SERIAL PRIMARY KEY,
	checked_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT now(),
	client_asset_fqdn TEXT NOT NULL,
	storage_asset_fqdn TEXT NOT NULL,
	storage_asset_path TEXT NOT NULL,
	mb_used INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS storage_usage_checked_at ON storage_usage (checked_at);
CREATE INDEX IF NOT EXISTS storage_usage_client_asset_fqdn ON storage_usage (client_asset_fqdn);
CREATE INDEX IF NOT EXISTS storage_usage_storage_asset_fqdn ON storage_usage (storage_asset_fqdn);
CREATE INDEX IF NOT EXISTS storage_usage_storage_asset_path ON storage_usage (storage_asset_path);
CREATE INDEX IF NOT EXISTS storage_usage_uniq_combo ON storage_usage (client_asset_fqdn, storage_asset_fqdn, storage_asset_path);


CREATE TABLE IF NOT EXISTS pipeline_salt_cmd_history (
	id SERIAL PRIMARY KEY,
	created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT now(),
	target TEXT NOT NULL,
	pipeline_id TEXT,
	pipeline_url TEXT NOT NULL,
	pipeline_status TEXT,
	project TEXT NOT NULL,
	timeout TEXT NOT NULL,
	cmd TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS pipeline_salt_cmd_history_created_at ON pipeline_salt_cmd_history (created_at);
CREATE INDEX IF NOT EXISTS pipeline_salt_cmd_history_target ON pipeline_salt_cmd_history (target);
CREATE INDEX IF NOT EXISTS pipeline_salt_cmd_history_pipeline_id ON pipeline_salt_cmd_history (pipeline_id);
CREATE INDEX IF NOT EXISTS pipeline_salt_cmd_history_pipeline_status ON pipeline_salt_cmd_history (pipeline_status);
CREATE INDEX IF NOT EXISTS pipeline_salt_cmd_history_project ON pipeline_salt_cmd_history (project);
CREATE INDEX IF NOT EXISTS pipeline_salt_cmd_history_cmd ON pipeline_salt_cmd_history (cmd);
CREATE INDEX IF NOT EXISTS pipeline_salt_cmd_history_target_cmd_combo ON pipeline_salt_cmd_history (target, cmd);


CREATE TABLE IF NOT EXISTS pipeline_rsnapshot_backup_history (
	id SERIAL PRIMARY KEY,
	created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT now(),
	target TEXT NOT NULL,
	pipeline_id TEXT,
	pipeline_url TEXT NOT NULL,
	pipeline_status TEXT,
	project TEXT NOT NULL,
	timeout TEXT NOT NULL,
	rsnapshot_backup_type TEXT NOT NULL,
	ssh_host TEXT,
	ssh_port TEXT,
	ssh_jump TEXT
);

CREATE INDEX IF NOT EXISTS pipeline_salt_cmd_history_created_at ON pipeline_salt_cmd_history (created_at);
CREATE INDEX IF NOT EXISTS pipeline_salt_cmd_history_target ON pipeline_salt_cmd_history (target);
CREATE INDEX IF NOT EXISTS pipeline_salt_cmd_history_pipeline_id ON pipeline_salt_cmd_history (pipeline_id);
CREATE INDEX IF NOT EXISTS pipeline_salt_cmd_history_pipeline_status ON pipeline_salt_cmd_history (pipeline_status);
CREATE INDEX IF NOT EXISTS pipeline_salt_cmd_history_project ON pipeline_salt_cmd_history (project);
