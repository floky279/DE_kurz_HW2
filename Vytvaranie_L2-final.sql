--L2
--L2_product
--Source table: L1_product
--Source system: L1

CREATE OR REPLACE VIEW `secure-granite-455615-p8.L2.L2_product` AS
SELECT
  product_id,
  product_name,
  product_type,
  product_category
FROM `secure-granite-455615-p8.L1.L1_product`
-- Include only active product categories relevant for analysis
WHERE product_category IN ('product', 'rent');

--L2
--L2_branch
--Source table: L1_branch
--Source system: L1

CREATE OR REPLACE VIEW `secure-granite-455615-p8.L2.L2_branch` AS
SELECT
  branch_id,
  branch_name
FROM `secure-granite-455615-p8.L1.L1_branch`
-- Exclude unknown branches
WHERE branch_name != 'unknown';

--L2
--L2_invoice
--Source table: L1_invoice
--Source system: L1

CREATE OR REPLACE VIEW `secure-granite-455615-p8.L2.L2_invoice` AS
SELECT 
  invoice.invoice_id,
 -- invoice.invoice_previous_id, --zbytečné 
  invoice.contract_id,
  invoice.date_issue,
  invoice.due_date,
  invoice.paid_date,
  invoice.start_date,
  invoice.end_date,
  invoice.amount_w_vat,
  invoice.return_w_vat,
  -- Amount without VAT: if the value is ≤ 0, keep 0; otherwise divide by 1.2 (assumes 20% VAT)
  CASE
    WHEN invoice.amount_w_vat <= 0 THEN 0
    WHEN invoice.amount_w_vat > 0 THEN invoice.amount_w_vat / 1.2
  END AS amount_wo_vat_usd,
  invoice.insert_date AS insert_date,
  invoice.update_date,
  -- Order of invoice within each contract, based on issue date (used for sequencing)
  ROW_NUMBER() OVER (PARTITION BY invoice.contract_id ORDER BY invoice.date_issue ASC) AS invoice_order
FROM `secure-granite-455615-p8.L1.L1_invoice` invoice
INNER JOIN `secure-granite-455615-p8.L1.L1_contract` contract
  ON invoice.contract_id = contract.contract_id
-- Consider only issued invoices of type 'invoice'
WHERE invoice.invoice_type = 'invoice'
  AND flag_invoice_issued;

--L2
--L2_contract
--Source table: L1_contract
--Source system: L1
CREATE OR REPLACE VIEW `secure-granite-455615-p8.L2.L2_contract` AS
SELECT 
  contract_id,
  branch_id,
  contract_valid_from,
  contract_valid_to,
  registered_date,
  signed_date,
  activation_process_date,
  prolongation_date,
  registration_end_reason,
  flag_contract_prolonged,
  flag_send_email,
  contract_status
FROM `secure-granite-455615-p8.L1.L1_contract`
-- Include only contracts that have been formally registered
WHERE registered_date IS NOT NULL;

--L2
-- L2_PRODUCT_PURCHASE
--Source table: L1_PRODUCT_PURCHASE
--Source system: L1

CREATE OR REPLACE VIEW `secure-granite-455615-p8.L2.L2_product_purchase` AS
SELECT
  product_purchase_id,
  contract_id,
  product_id,
  product_category,
  product_status,
  create_date,
  product_valid_from,
  product_valid_to,
  price_wo_vat,
  -- Compute price including VAT: 0 if base price ≤ 0, otherwise apply 20% VAT
  IF(price_wo_vat <= 0, 0, price_wo_vat * 1.20) AS price_w_vat,
  product_unit,
  update_date,
  product_name,
  product_type,
  -- Identify lifetime/unlimited product: valid_from = 2035-12-31 is treated as unlimited
  IF(product_valid_from = '2035-12-31', TRUE, FALSE) AS flag_unlimited_product
FROM `secure-granite-455615-p8.L1.L1_product_purchase`
-- Only active product purchases of interest (not canceled/disconnected)
WHERE product_status NOT IN ('canceled', 'canceled registration', 'disconnected')
  AND product_status IS NOT NULL
  AND product_category IN ('product', 'rent');

