--L3_contract

-- DIM_CONTRACT 
CREATE OR REPLACE VIEW
  `secure-granite-455615-p8.L3_snowflake.L3_contract` AS
SELECT
  contract_id,
  branch_id,
  contract_valid_from,
  contract_valid_to,
  prolongation_date,
  registration_end_reason,
  contract_status,

   -- BUSINESS LOGIC: bucket contract length into 4 categories
  --TOTO BY ZDE NEMUSELO VŮBEC BÝT
  CASE
    WHEN DATE_DIFF(contract_valid_to, contract_valid_from, MONTH) <  6 THEN 'less than half year'
    WHEN DATE_DIFF(contract_valid_to, contract_valid_from, MONTH) BETWEEN  6 AND 12 THEN '1 year'
    WHEN DATE_DIFF(contract_valid_to, contract_valid_from, MONTH) BETWEEN 13 AND 24 THEN '2 years'
    WHEN DATE_DIFF(contract_valid_to, contract_valid_from, MONTH)  > 24 THEN 'more than 2 years'
    ELSE 'unknown'
  END                                        AS contract_duration,

  -- BUSINESS LOGIC: year dimension for easier reporting
  EXTRACT(YEAR  FROM contract_valid_from)    AS start_year_of_contract,
  -- BUSINESS LOGIC: flag contracts that have been prolonged
  (prolongation_date IS NOT NULL)            AS flag_prolongation
  FROM `secure-granite-455615-p8.L2.L2_contract`
  --Business logic: exclude invalid or missing dates
  WHERE contract_valid_from IS NOT NULL
  AND contract_valid_to   IS NOT NULL
  AND contract_valid_to  >= contract_valid_from;

--L3_product
--DIM_PRODUCT
CREATE OR REPLACE VIEW
  `secure-granite-455615-p8.L3_snowflake.L3_product` AS
SELECT
  pp.product_purchase_id,           
  pp.product_id,
  p.product_name,
  p.product_type,
  pp.product_valid_from,
  pp.product_valid_to,
  pp.product_unit   AS unit,
  pp.flag_unlimited_product
FROM `secure-granite-455615-p8.L2.L2_product_purchase` AS pp
LEFT JOIN `secure-granite-455615-p8.L2.L2_product`       AS p
       ON pp.product_id = p.product_id
-- BUSINESS RULE: keep only rows with a populated package / product name
WHERE p.product_name IS NOT NULL; 

--L3_branch
-- DIM_BRANCH

CREATE OR REPLACE VIEW
  `secure-granite-455615-p8.L3_snowflake.L3_branch` AS
SELECT
  branch_id,
  branch_name
FROM `secure-granite-455615-p8.L2.L2_branch`;


--L3_invoice
--FACT_INVOICE

CREATE OR REPLACE VIEW
  `secure-granite-455615-p8.L3_snowflake.L3_invoice` AS
SELECT
  inv.invoice_id,
  inv.contract_id,
  pp.product_id,                                    
  inv.amount_w_vat,
  inv.return_w_vat,
  -- BUSINESS LOGIC: total paid = amount minus returns,
  -- NULLs converted to zero so the metric is always numeric
  COALESCE(inv.amount_w_vat, 0)
    - COALESCE(inv.return_w_vat, 0)   AS total_paid,
  inv.paid_date
FROM `secure-granite-455615-p8.L2.L2_invoice`           AS inv
LEFT JOIN `secure-granite-455615-p8.L2.L2_product_purchase` AS pp
       ON pp.contract_id = inv.contract_id; 
