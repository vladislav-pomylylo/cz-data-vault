-- Проверка: нет дубликатов stop_id
SELECT stop_id, COUNT(*)
FROM bronze.gtfs_stops
GROUP BY stop_id
HAVING COUNT(*) > 1;

-- Проверка: координаты в пределах Праги
SELECT *
FROM bronze.gtfs_stops
WHERE stop_lat < 49.8 OR stop_lat > 50.3
   OR stop_lon < 14.0 OR stop_lon > 14.9;
