/*
===============================================================================
Quality Checks for the Gold Layer
===============================================================================
WHY?
Various checks are performed on each virtual table (i.e. view), to ensure data integrity, consistency, and accuracy in the Gold Layer of the data pipeline.

HOW?
- check for uniqueness of surrogate keys in dimension tables
- validate referential integrity between fact and dimension tables
- validate relationships in the data model for analytical purposes

EXPECTED OUTCOMES:
Running ANY of these queries should return NO RESULTS, indicating that the data meets the defined quality standards.
===============================================================================
*/

/*
====================================================================
Checking 'gold.dim_customers'
====================================================================
*/
-- Check for Uniqueness of Customer Key in gold.dim_customers
SELECT 
    customer_key,
    COUNT(*) AS duplicate_count
FROM gold.dim_customers
GROUP BY customer_key
HAVING COUNT(*) > 1;

/*
====================================================================
Checking 'gold.product_key'
====================================================================
*/
-- Check for Uniqueness of Product Key in gold.dim_products
SELECT 
    product_key,
    COUNT(*) AS duplicate_count
FROM gold.dim_products
GROUP BY product_key
HAVING COUNT(*) > 1;

/*
====================================================================
Checking 'gold.fact_sales'
====================================================================
*/
-- Check the data model connectivity between fact and dimensions
SELECT * 
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c ON c.customer_key = f.customer_key
LEFT JOIN gold.dim_products p ON p.product_key = f.product_key
WHERE p.product_key IS NULL OR c.customer_key IS NULL
