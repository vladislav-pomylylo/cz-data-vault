MODEL (
  name bronze.gtfs_stops,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column _ingested_at,
    batch_size 1
  ),
  audits (
    not_null(columns := (stop_id))
  ),
  owner data,
  cron '@weekly',
  description 'GTFS stops for Prague public transport'
);

SELECT
  stop_id,
  stop_name,
  stop_lat,
  stop_lon,
  stop_code,
  zone_id,
  _ingested_at
FROM
  raw.gtfs_stops
WHERE
  _ingested_at BETWEEN @start_date AND @end_date;
