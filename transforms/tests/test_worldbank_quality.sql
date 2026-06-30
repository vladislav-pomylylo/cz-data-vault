-- Проверка: GDP не может быть отрицательным
SELECT *
FROM bronze.worldbank
WHERE indicator_name = 'gdp_usd' AND value < 0;

-- Проверка: безработица в разумном диапазоне (0-50%)
SELECT *
FROM bronze.worldbank
WHERE indicator_name = 'unemployment_pct' AND (value < 0 OR value > 50);

-- Проверка: население больше 1000 человек
SELECT *
FROM bronze.worldbank
WHERE indicator_name = 'population' AND value < 1000;
