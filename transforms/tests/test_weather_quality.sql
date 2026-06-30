-- Проверка: температура в разумном диапазоне (-50..+50)
SELECT *
FROM bronze.weather
WHERE temperature_c < -50 OR temperature_c > 50;
