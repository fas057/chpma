-- 1. Создаем таблицу сырых событий
CREATE TABLE user_events
(
    user_id UInt32,
    event_type String,
    points_spent UInt32,
    event_time DateTime
)
ENGINE = MergeTree()
ORDER BY (event_time, user_id)
TTL event_time + INTERVAL 30 DAY;

-- 2. Создаем агрегированную таблицу
CREATE TABLE user_events_aggregated
(
    event_date Date,
    event_type String,
    users AggregateFunction(uniq, UInt32),
    points_spent AggregateFunction(sum, UInt32),
    actions_count AggregateFunction(count, UInt32)
)
ENGINE = SummingMergeTree()
ORDER BY (event_date, event_type)
TTL event_date + INTERVAL 180 DAY;

-- 3. Создаем Materialized View
CREATE MATERIALIZED VIEW user_events_mv TO user_events_aggregated AS
SELECT
    toDate(event_time) AS event_date,
    event_type,
    uniqState(user_id) AS users,
    sumState(points_spent) AS points_spent,
    countState() AS actions_count
FROM user_events
GROUP BY event_date, event_type;

-- 4. Вставляем тестовые данные
INSERT INTO user_events VALUES
(1, 'login', 0, now() - INTERVAL 10 DAY),
(2, 'signup', 0, now() - INTERVAL 10 DAY),
(3, 'login', 0, now() - INTERVAL 10 DAY),
(1, 'login', 0, now() - INTERVAL 7 DAY),
(2, 'login', 0, now() - INTERVAL 7 DAY),
(3, 'purchase', 30, now() - INTERVAL 7 DAY),
(1, 'purchase', 50, now() - INTERVAL 5 DAY),
(2, 'logout', 0, now() - INTERVAL 5 DAY),
(4, 'login', 0, now() - INTERVAL 5 DAY),
(1, 'login', 0, now() - INTERVAL 3 DAY),
(3, 'purchase', 70, now() - INTERVAL 3 DAY),
(5, 'signup', 0, now() - INTERVAL 3 DAY),
(2, 'purchase', 20, now() - INTERVAL 1 DAY),
(4, 'logout', 0, now() - INTERVAL 1 DAY),
(5, 'login', 0, now() - INTERVAL 1 DAY),
(1, 'purchase', 25, now()),
(2, 'login', 0, now()),
(3, 'logout', 0, now()),
(6, 'signup', 0, now()),
(6, 'purchase', 100, now());

-- 5. Запрос для расчета Retention (7-дневный)
WITH 
-- Получаем всех уникальных пользователей по дням
daily_active_users AS (
    SELECT 
        toDate(event_time) AS day,
        groupUniqArray(user_id) AS active_users
    FROM user_events
    GROUP BY day
),

-- Собираем все дни, когда пользователи были активны
user_activity_days AS (
    SELECT
        user_id,
        groupArray(toDate(event_time)) AS activity_days
    FROM user_events
    GROUP BY user_id
),

-- Находим для каждого дня всех пользователей, которые вернулись в течение 7 дней
retention_calculation AS (
    SELECT
        d1.day AS cohort_day,
        length(d1.active_users) AS cohort_size,
        countIf(uad.activity_days, 
            arrayExists(x -> (x >= (cohort_day + toIntervalDay(1)) AND x <= (cohort_day + toIntervalDay(7))), uad.activity_days)
        ) AS retained_users
    FROM daily_active_users d1
    JOIN user_activity_days uad ON has(d1.active_users, uad.user_id)
    GROUP BY cohort_day, cohort_size
)

-- Итоговый отчет
SELECT
    cohort_day AS day_0,
    cohort_size AS total_users_day_0,
    retained_users AS returned_in_7_days,
    round((retained_users / cohort_size) * 100, 2) AS retention_7d_percent,
    concat(toString(cohort_size), '|', toString(retained_users), '|', toString(round((retained_users / cohort_size) * 100, 2)), '%') AS retention_format
FROM retention_calculation
ORDER BY day_0;

-- 6. Пример аналитического запроса
SELECT
    event_date,
    event_type,
    uniqMerge(users) AS unique_users,
    sumMerge(points_spent) AS total_points_spent,
    countMerge(actions_count) AS total_actions
FROM user_events_aggregated
GROUP BY event_date, event_type
ORDER BY event_date DESC, event_type;
