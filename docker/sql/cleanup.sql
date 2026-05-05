DELETE FROM test_dtt_transactions
WHERE value_date < CURRENT_DATE - INTERVAL '7 days';