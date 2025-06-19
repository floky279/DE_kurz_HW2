--L1_google_sheet
--L1_status
--Source table: L0_google_sheet.status
--Source system: google_sheet

CREATE OR REPLACE VIEW `secure-granite-455615-p8.L1.L1_status` 
AS
SELECT 
CAST(id_status AS INT) AS product_status_id-- PK
,LOWER(status_name) AS product_status_name
,DATE(TIMESTAMP(date_update),"UTC+2") AS product_status_update_date --teoreticky to tu ani nemusí být :) 
--nefungoval EUROPE/PRAGUE, musela som použiť UTC časové pásmo, dala som tam na letný čas UTC+2 --> super řešení!!!
FROM `secure-granite-455615-p8.L0_google_sheet.status` 
WHERE id_status IS NOT NULL
AND status_name IS NOT NULL
QUALIFY ROW_NUMBER() OVER(PARTITION BY id_status) = 1
--unique id
;


-- L1_product
--Source table: L0_google_sheet.product
--Source system: google_sheet

CREATE OR REPLACE VIEW `secure-granite-455615-p8.L1.L1_product` AS
SELECT
  id_product AS product_id,              -- PK
  LOWER(name) AS product_name,                  -- Normalize text to lowercase to ensure consistency
  LOWER (type) AS product_type,                  -- Product type (e.g. service, good)
  LOWER (category) AS product_category,          -- Product category (e.g. software, accessories)
 -- is_vat_applicable AS is_vat_applicable,-- TRUE if VAT applies, FALSE if exempt
 -- date_update AS product_update_date     --nemusí tu být

FROM `secure-granite-455615-p8.L0_google_sheet.product`
WHERE id_product IS NOT NULL 
  AND name IS NOT NULL -- Filter out NULL product_id (required as primary key) and NULL name (essential for reporting/labels).
QUALIFY ROW_NUMBER() OVER(PARTITION BY id_product) = 1; --It has multiple versions of PK, we need just the unique ones

--L1_branch
--Source table: L0_google_sheet.status.branch
--Source system: google_sheet

CREATE OR REPLACE VIEW `secure-granite-455615-p8.L1.L1_branch` AS
SELECT
  CAST(id_branch AS INT) AS branch_id,                   -- PK
  LOWER(branch_name) AS branch_name,              
  date_update AS product_status_update_date --nemusí to být, opakuje se to

FROM `secure-granite-455615-p8.L0_google_sheet.branch`
WHERE id_branch != "NULL" -- Filter out NULL values in PK
;



-- L1 accounting_system
-- L1_invoice
--Source table: L0_accounting_system.invoice
--Source system: accounting_system

CREATE OR REPLACE VIEW `secure-granite-455615-p8.L1.L1_invoice` AS
SELECT
  DATE(date, "UTC+2") AS date_issue,
  DATE(scadent, "UTC+2") AS due_date,
  DATE(date_paid, "UTC+2") AS paid_date,
  DATE(start_date, "UTC+2") AS start_date,
  DATE(end_date, "UTC+2") AS end_date,
  DATE(date_insert, "UTC+2") AS insert_date,
  DATE(date_update, "UTC+2") AS update_date,

  invoice_type AS invoice_type_id,  -- Invoice_type: 1 - invoice, 3 - credit_note, 2 - return, 4 - other
  CASE
    WHEN invoice_type = 1 THEN "invoice"
    WHEN invoice_type = 2 THEN "return"
    WHEN invoice_type = 3 THEN "credit_note"
    WHEN invoice_type = 4 THEN "other"
  END AS invoice_type,

  id_invoice AS invoice_id,  -- PK
  id_invoice_old AS invoice_previous_id,  -- Id of previous invoice. if it's not null, means that current invoice id is credit note or return of the id_invoice_old

  invoice_id_contract AS contract_id,  -- FK
  id_branch AS branch_id,  -- FK

  status AS invoice_status_id, 
  CASE --mohlo by být použití IF viz.  IF(status < 100, TRUE, FALSE) AS flag_invoice_issued, 
    WHEN status < 100 THEN TRUE
    ELSE FALSE
  END AS flag_invoice_issued,  --Invoce status. Invoice status < 100  have been issued. >= 100 - not issued

  value AS amount_w_vat,
  payed AS amount_payed,
  flag_paid_currier AS flag_paid_currier,
  CAST(number AS INT) AS invoice_number,--v L0 it is in format string
  value_storno AS return_w_vat

FROM `secure-granite-455615-p8.L0_accounting_system.invoice`;



-- L1_invoice_load
--Source table: L0_accounting_system.invoice_load
--Source system: accounting_system

CREATE OR REPLACE VIEW `secure-granite-455615-p8.L1.L1_invoice_load` AS
SELECT
  id_load AS invoice_load_id,  -- PK
  id_contract AS contract_id,  -- FK: contract
  CAST(id_package AS INT) AS package_id,  -- FK: purchased package
  CAST(id_package_template AS INT) AS product_id,  -- FK: product
  id_invoice AS invoice_id,  -- FK: invoice

  notlei AS price_wo_vat_usd,  -- Price without VAT
  value AS price_w_vat_usd,  -- Price with VAT
  payed AS paid_w_vat_usd,  -- Paid amount with VAT
  tva AS vat_rate,  
  LOWER(currency) AS currency_code,  -- Currency (EUR, USD)

  CASE 
    WHEN um IN ('mesia','m?síce','m?si?1ce','měsice','mesiace','měsíce','mesice') THEN 'month'
    WHEN um = 'kus' THEN 'item'
    WHEN um = 'den' THEN 'day'
    WHEN um = '0' THEN NULL
    ELSE um 
  END AS unit,                              -- Normalized unit; Unit of measure (pcs, kg); used for interpreting quantity values in calculations and reporting
  quantity AS quantity, 

  DATE(start_date, "UTC+2") AS start_date,  -- Start of invoiced period
  DATE(end_date, "UTC+2") AS end_date,  -- End of invoiced period
  DATE(date_insert, "UTC+2") AS insert_date, 
  DATE(date_update, "UTC+2") AS update_date,  
  --load_date AS load_date  -- Load date (already DATE)

FROM `secure-granite-455615-p8.L0_accounting_system.invoice_load`;


--L1_crm
-- L1_contract
--Source table: L0_crm.contract
--Source system: crm

CREATE OR REPLACE VIEW `secure-granite-455615-p8.L1.L1_contract` AS
SELECT
  id_contract AS contract_id,  -- PK
  id_branch AS branch_id,      -- FK
  DATE(date_contract_valid_from, "UTC+2") AS contract_valid_from,
  DATE(TIMESTAMP(PARSE_DATE('%Y-%m-%d', date_contract_valid_to)), "UTC+2") AS contract_valid_to,
  DATE(date_registered, "UTC+2") AS registered_date,
  DATE(date_signed, "UTC+2") AS signed_date,
  DATE(activation_process_date, "UTC+2") AS activation_process_date,
  DATE(prolongation_date, "UTC+2") AS prolongation_date,
  LOWER(registration_end_reason) AS registration_end_reason,
  flag_prolongation AS flag_contract_prolonged,
  flag_send_inv_email AS flag_send_email,
  LOWER(contract_status) AS contract_status,
  load_date AS load_date 
FROM `secure-granite-455615-p8.L0_crm.contract`;


--product_purchase
--Source table: L0_crm.contract.purchase
--Source system: crm

CREATE OR REPLACE VIEW `secure-granite-455615-p8.L1.L1_product_purchase` AS
SELECT
  p.id_package AS product_purchase_id,          -- PK: unique package purchase ID
  p.id_contract AS contract_id,                 -- FK: contract
  p.id_package_template AS product_id,          -- FK: product template
  pr.product_name AS product_name,            
  pr.product_type AS product_type,
  pr.product_category AS product_category,
  

  DATE(p.date_insert, "UTC+2") AS create_date,
  DATE(TIMESTAMP(PARSE_DATE('%Y-%m-%d', p.start_date)), "UTC+2") AS product_valid_from,
  DATE(TIMESTAMP(PARSE_DATE('%Y-%m-%d', p.end_date)), "UTC+2") AS product_valid_to,
  DATE(p.date_update, "UTC+2") AS update_date,

 
  p.fee AS price_wo_vat,
  p.package_status AS product_status_id,
  s.product_status_name AS product_status,      -- Text name from status 

 /* CASE --toto není potřeba, již to jednou máme!
    WHEN p.measure_unit IN ('mesia','m?síce','m?si?1ce','měsice','mesiace','měsíce','mesice') THEN 'month'
    WHEN p.measure_unit = 'kus' THEN 'item'
    WHEN p.measure_unit = 'den' THEN 'day'
    WHEN p.measure_unit = '0' THEN NULL
    ELSE LOWER(p.measure_unit)
  END AS product_unit, -- same logic as in invoice_load*/
  
 -- p.id_branch AS branch_id,
  --p.load_date AS load_date                      -- Already DATE

FROM `secure-granite-455615-p8.L0_crm.product_purchase` AS p

-- Join on product 
LEFT JOIN `secure-granite-455615-p8.L1.L1_product` AS pr
  ON p.id_package_template = pr.product_id
--pridavala som ho kvoli obchodnym informáciám o produkte

-- Join on status 
LEFT JOIN `secure-granite-455615-p8.L1.L1_status` AS s
  ON p.package_status = s.product_status_id;



