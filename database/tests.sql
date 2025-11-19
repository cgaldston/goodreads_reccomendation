-- 1) nulls
SELECT user_id, COUNT(book_id) as total_reviews
FROM interactions
GROUP BY user_id
ORDER BY total_reviews DESC
LIMIT 5;


