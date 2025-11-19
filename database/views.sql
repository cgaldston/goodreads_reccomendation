-- active users/items (you already used these)
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_users_ge10 AS
SELECT user_id FROM interactions GROUP BY 1 HAVING COUNT(*) >= 10;
CREATE UNIQUE INDEX IF NOT EXISTS ux_mv_users_ge10 ON mv_users_ge10(user_id);

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_books_ge30 AS
SELECT book_id FROM interactions GROUP BY 1 HAVING COUNT(*) >= 30;
CREATE UNIQUE INDEX IF NOT EXISTS ux_mv_books_ge30 ON mv_books_ge30(book_id);

-- candidate items for scoring (popularity floor = 50; tweak)
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_popular_items AS
SELECT book_id, COUNT(*) AS n_ratings
FROM interactions
GROUP BY book_id
HAVING COUNT(*) >= 50
ORDER BY n_ratings DESC;

CREATE INDEX IF NOT EXISTS idx_mv_popular_items ON mv_popular_items(book_id);

-- refresh these after big loads:
-- REFRESH MATERIALIZED VIEW CONCURRENTLY mv_users_ge10;
-- REFRESH MATERIALIZED VIEW CONCURRENTLY mv_books_ge30;
-- REFRESH MATERIALIZED VIEW CONCURRENTLY mv_popular_items;
