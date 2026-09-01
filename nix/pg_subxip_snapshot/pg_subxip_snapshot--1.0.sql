\echo Use "CREATE EXTENSION pg_subxip_snapshot" to load this file. \quit

CREATE FUNCTION pg_current_subxip_snapshot_text()
RETURNS pg_catalog.text
AS 'MODULE_PATHNAME', 'pg_current_subxip_snapshot_text'
LANGUAGE C
STABLE
PARALLEL RESTRICTED;
