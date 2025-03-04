/* Анализ рынка недвижимости Ленинградской области.
 
   Определим ключевые метрики и выведем топ 15 населенных пунктов. 
 
   Описание данных

Таблица advertisement
Содержит информацию об объявлениях:
id — идентификатор объявления (первичный ключ).
first_day_exposition — дата подачи объявления.
days_exposition — длительность нахождения объявления на сайте (в днях).
last_price — стоимость квартиры в объявлении, в руб.

Таблица flats
Содержит информацию о квартирах:
id — идентификатор квартиры (первичный ключ, связан с первичным ключом id таблицы advertisement).
city_id — идентификатор города (внешний ключ, связан с city_id таблицы city).
type_id — идентификатор типа населённого пункта (внешний ключ, связан с type_id таблицы type).
total_area — общая площадь квартиры, в кв. метрах.
rooms — число комнат.
ceiling_height — высота потолка, в метрах.
floors_total — этажность дома, в котором находится квартира.
living_area — жилая площадь, в кв. метрах.
floor — этаж квартиры.
is_apartment — указатель, является ли квартира апартаментами (1 — является, 0 — не является).
open_plan — указатель, имеется ли в квартире открытая планировка (1 — открытая планировка квартиры, 0 — открытая планировка отсутствует).
kitchen_area — площадь кухни, в кв. метрах.
balcony — количество балконов в квартире.
airports_nearest — расстояние до ближайшего аэропорта, в метрах.
parks_around3000 — число парков в радиусе трёх километров.
ponds_around3000 — число водоёмов в радиусе трёх километров.

Таблица city
Содержит информацию о городах:
city_id — идентификатор населённого пункта (первичный ключ).
city — название населённого пункта.

Таблица type
Содержит информацию о городах:
type_id — идентификатор типа населённого пункта (первичный ключ).
type — название типа населённого пункта.

*/


WITH limits AS (
    -- Вычисляем границы выбросов (99-й и 1-й перцентили) для характеристик недвижимости на основании представлениях о рынке и исходя из ознакомелния данными
    SELECT  
        PERCENTILE_DISC(0.99) WITHIN GROUP (ORDER BY total_area) AS total_area_limit,  -- 99-й перцентиль для площади
        PERCENTILE_DISC(0.99) WITHIN GROUP (ORDER BY rooms) AS rooms_limit,            -- 99-й перцентиль для количества комнат
        PERCENTILE_DISC(0.99) WITHIN GROUP (ORDER BY balcony) AS balcony_limit,        -- 99-й перцентиль для балкона
        PERCENTILE_DISC(0.99) WITHIN GROUP (ORDER BY ceiling_height) AS ceiling_height_limit_h,  -- 99-й перцентиль для высоты потолков (верхний предел)
        PERCENTILE_DISC(0.01) WITHIN GROUP (ORDER BY ceiling_height) AS ceiling_height_limit_l   -- 1-й перцентиль для высоты потолков (нижний предел)
    FROM real_estate.flats  -- Источник данных: таблица flats (квартиры)
),
data_len_obl AS (
    -- Анализируем объявления, отфильтрованные по условиям (без выбросов)
    SELECT 
        c.city_id,               -- ID города
        c.city,                  -- Название города
        t.type,                  -- Тип недвижимости (например, квартира, дом)
        COUNT(a.id) AS count_publication,  -- Количество опубликованных объявлений
        COUNT(a.days_exposition) AS count_removal,  -- Количество снятых объявлений
        AVG(f.total_area) AS avg_area,  -- Средняя площадь
        AVG(a.last_price) AS avg_price,  -- Средняя цена
        AVG(a.days_exposition) AS avg_days_exposition  -- Средняя длительность публикации объявления
    FROM real_estate.flats AS f  -- Основная таблица: квартиры
    JOIN limits l ON true  -- Присоединяем лимиты (состоят из 1 строки, используются в фильтрации)
    LEFT JOIN real_estate.city AS c USING(city_id)  -- Присоединяем информацию о городе
    LEFT JOIN real_estate.type AS t USING(type_id)  -- Присоединяем информацию о типе недвижимости
    LEFT JOIN real_estate.advertisement AS a USING(id)  -- Присоединяем информацию о объявлении
    WHERE 
        f.total_area < l.total_area_limit  -- Фильтруем по площади (меньше 99-го перцентиля)
        AND (f.rooms < l.rooms_limit OR f.rooms IS NULL)  -- Фильтруем по количеству комнат (меньше 99-го перцентиля или NULL)
        AND (f.balcony < l.balcony_limit OR f.balcony IS NULL)  -- Фильтруем по наличию балкона (меньше 99-го перцентиля или NULL)
        AND ((f.ceiling_height BETWEEN l.ceiling_height_limit_l AND l.ceiling_height_limit_h) OR f.ceiling_height IS NULL)  -- Фильтруем по высоте потолков (между 1-м и 99-м перцентилями или NULL)
        AND c.city != 'Санкт-Петербург'  -- Исключаем Санкт-Петербург из результатов (частный случай надо рассматривать отдельно, сильно искажает общую картину)
    GROUP BY c.city_id, c.city, t.type  -- Группируем по городу и типу недвижимости
),
ranked_data AS (
    -- Добавляем ранжирование по количеству публикаций 
    SELECT *, 
        DENSE_RANK() OVER (ORDER BY count_publication DESC) AS rnk  -- Ранжируем по убыванию количества публикаций
    FROM data_len_obl  -- Источник данных: временная таблица с обработанными данными
)
-- Выводим финальные результаты
SELECT 
    city,  -- Город
    type,  -- Тип недвижимости
    count_publication,  -- Количество публикаций
    rnk,  -- Рейтинг по количеству публикаций
    (count_removal / count_publication::NUMERIC)::NUMERIC(4,2) AS per_of_sale,  -- Доля проданных объектов (снятых объявлений)
    avg_area::NUMERIC(5,1),  -- Средняя площадь
    (avg_price / avg_area)::NUMERIC(10,2) AS price_m2,  -- Средння цена за квадратный метр
    avg_days_exposition::NUMERIC(5,1)  -- Среднее количество дней на экспозиции
FROM ranked_data  -- Источник данных: таблица с ранжированием
WHERE rnk < 16  -- Отбираем только топ-15 по количеству публикаций
ORDER BY count_publication DESC;  -- Сортируем по количеству публикаций по убыванию
