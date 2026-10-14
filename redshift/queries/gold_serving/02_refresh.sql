-- Repeatable, all-or-nothing full refresh for the small portfolio dataset.
-- The stored procedure stages every selected Gold table and preserves all history.
CALL retail_serving.refresh_gold_serving();
