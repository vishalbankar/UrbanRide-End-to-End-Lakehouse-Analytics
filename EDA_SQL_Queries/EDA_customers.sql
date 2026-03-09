
select * from urbanride.bronze.customers limit 10;

-------- duplicate data ----
SELECT count(*)
FROM urbanride.bronze.customers
WHERE (customer_id, signup_date) IN (
  SELECT customer_id, signup_date
  FROM urbanride.bronze.customers
  GROUP BY customer_id, signup_date
  HAVING COUNT(*) > 1
);


-------- invalid ratings 

select * from urbanride.bronze.customers where rating <0;

------- Flags rows where is_churned=True but churn_date=NULL or is_churned=False but churn_date is set. Doesn't drop them — flags with is_churn_inconsistent
SELECT *,
  CASE
    WHEN (is_churned = TRUE AND churn_date IS NULL)
      OR (is_churned = FALSE AND churn_date IS NOT NULL)
    THEN TRUE
    ELSE FALSE
  END AS is_churn_inconsistent
FROM urbanride.bronze.customers;
