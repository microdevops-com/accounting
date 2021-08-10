CREATE TABLE IF NOT EXISTS issues_checked (
	id SERIAL PRIMARY KEY,
	issue_id INTEGER NOT NULL,
	checked_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT now(),
	transaction_id BIGINT NOT NULL DEFAULT txid_current()
);

CREATE INDEX IF NOT EXISTS issues_checked_issue_id ON issues_checked (issue_id);
CREATE INDEX IF NOT EXISTS issues_checked_checked_at ON issues_checked (checked_at);
CREATE INDEX IF NOT EXISTS issues_checked_transaction_id ON issues_checked (transaction_id);


CREATE TABLE IF NOT EXISTS hourly_employee_timelogs_checked (
	id SERIAL PRIMARY KEY,
	timelog_id INTEGER NOT NULL UNIQUE,
	checked_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT now(),
	transaction_id BIGINT NOT NULL DEFAULT txid_current()
);

CREATE INDEX IF NOT EXISTS hourly_employee_timelogs_checked_checked_at ON hourly_employee_timelogs_checked (checked_at);
CREATE INDEX IF NOT EXISTS hourly_employee_timelogs_checked_transaction_id ON hourly_employee_timelogs_checked (transaction_id);


CREATE TABLE IF NOT EXISTS hourly_issue_timelogs_checked (
	id SERIAL PRIMARY KEY,
	timelog_id INTEGER NOT NULL UNIQUE,
	checked_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT now(),
	transaction_id BIGINT NOT NULL DEFAULT txid_current()
);

CREATE INDEX IF NOT EXISTS hourly_issue_timelogs_checked_checked_at ON hourly_issue_timelogs_checked (checked_at);
CREATE INDEX IF NOT EXISTS hourly_issue_timelogs_checked_transaction_id ON hourly_issue_timelogs_checked (transaction_id);


CREATE TABLE IF NOT EXISTS storage_usage (
	id SERIAL PRIMARY KEY,
	checked_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT now(),
	client_server_fqdn TEXT NOT NULL,
	storage_server_fqdn TEXT NOT NULL,
	storage_server_path TEXT NOT NULL,
	mb_used INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS storage_usage_checked_at ON storage_usage (checked_at);
CREATE INDEX IF NOT EXISTS storage_usage_client_server_fqdn ON storage_usage (client_server_fqdn);
CREATE INDEX IF NOT EXISTS storage_usage_storage_server_fqdn ON storage_usage (storage_server_fqdn);
CREATE INDEX IF NOT EXISTS storage_usage_storage_server_path ON storage_usage (storage_server_path);
CREATE INDEX IF NOT EXISTS storage_usage_uniq_combo ON storage_usage (client_server_fqdn, storage_server_fqdn, storage_server_path);
