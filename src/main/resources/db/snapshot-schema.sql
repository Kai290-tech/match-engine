-- MySQL schema for engine snapshots
CREATE TABLE IF NOT EXISTS engine_snapshot (
  id BIGINT PRIMARY KEY AUTO_INCREMENT,
  schema_version SMALLINT NOT NULL,
  topic VARCHAR(128) NOT NULL,
  partition_id INT NOT NULL,
  last_fully_processed_offset BIGINT NOT NULL,
  created_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  UNIQUE KEY uk_tp_off (topic, partition_id, last_fully_processed_offset),
  KEY idx_tp_latest (topic, partition_id, last_fully_processed_offset DESC)
);

CREATE TABLE IF NOT EXISTS engine_snapshot_section (
  id BIGINT PRIMARY KEY AUTO_INCREMENT,
  snapshot_id BIGINT NOT NULL,
  section_type VARCHAR(32) NOT NULL,
  section_key VARCHAR(64) NOT NULL DEFAULT '',
  encoding VARCHAR(16) NOT NULL,
  payload MEDIUMBLOB NOT NULL,
  UNIQUE KEY uk_snap_type_key (snapshot_id, section_type, section_key),
  KEY idx_snap (snapshot_id)
);
