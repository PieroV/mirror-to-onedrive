PRAGMA foreign_keys = 1;

CREATE TABLE IF NOT EXISTS item (
	onedrive_id TEXT PRIMARY KEY,
	onedrive_name TEXT,
	original_path TEXT,
	existing INTEGER DEFAULT 1 NOT NULL,
	is_folder INTEGER DEFAULT 0 NOT NULL,
	size INTEGER,
	mdate REAL,
	hash TEXT,
	parent_id TEXT,
	FOREIGN KEY(parent_id) REFERENCES item(onedrive_id) ON UPDATE CASCADE ON DELETE CASCADE
);

CREATE INDEX parents ON item(parent_id);
