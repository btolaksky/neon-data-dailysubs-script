
WITH vip AS
(
SELECT DISTINCT a.account_id, a.first_name, a.last_name, a.email, 'PROMO' AS activation_method, p.name AS promotion_name, op.srv_start_date AS service_start_date, op.validity_end_date AS service_end_date, eas.description AS status
			FROM loaded.ev_op_promotions_vw opp
			INNER JOIN loaded.ev_promotions_vw p ON opp.promotion_id = p.id
			INNER JOIN loaded.ev_ordered_product_vw op ON opp.ord_prod_id = op.ord_prod_id
			INNER JOIN loaded.ev_account_vw ea ON op.acct_id = ea.acct_id
			INNER JOIN dwh.dim_account a ON ea.customer_id = a.account_id
			LEFT JOIN loaded.ev_account_status_vw eas ON ea.acct_status_id = eas.acct_stat_id
			WHERE p.name IN ('VIP','SKY Employee Perk')
			AND a.account_id NOT IN (SELECT DISTINCT account_id FROM neon_reporting.test_accounts)
			AND DATE(GETDATE() - interval '1 days') BETWEEN DATE(service_start_date) AND DATE(service_end_date)
			AND status = 'Active'
),

paying AS (
SELECT DISTINCT account_id, dim_service_key, s.product_name, activation_method, s.service_status, s.service_start_date, s.service_end_date, s.coupon_code
FROM dwh.dim_service s
INNER JOIN dwh.dim_date d ON DATE(d.date_dt) >= DATE(s.service_start_date) AND DATE(d.date_dt) <= DATE(s.service_end_date)
WHERE d.date_dt = DATE(GETDATE() - interval '1 days')
AND s.product_name IN ('Standard', 'Premium', 'Annual', 'Basic')
AND s.service_status NOT IN ('EXPIRED','INACTIVE')
AND s.activation_method NOT IN ('FREE TRIAL', 'VOUCHER')
--AND DATE(s.service_start_date) >= DATE(GETDATE() - interval '1 days')
AND s.account_id NOT IN (SELECT DISTINCT account_id FROM neon_reporting.test_accounts)
AND s.account_id NOT IN (SELECT DISTINCT account_id FROM vip)
ORDER BY account_id, dim_service_key
)

-----------------------------------------------


SELECT DISTINCT DATE(GETDATE() - interval '1 days') AS date
, 1 AS "ORDER"
, 'New Monthly Free Trialists' AS measure
, COUNT(DISTINCT account_id) AS value
FROM
(
	SELECT DISTINCT s.account_id, s.dim_service_key, s.product_name, s.activation_method, s.service_status, s.service_start_date, s.service_end_date, s.coupon_code FROM dwh.dim_service s
	INNER JOIN dwh.dim_date d ON DATE(d.date_dt) >= DATE(s.service_start_date) AND DATE(d.date_dt) <= DATE(s.service_end_date)
	WHERE d.date_dt = DATE(GETDATE() - interval '1 days')
	AND s.product_name IN ('Standard', 'Premium')
	AND s.service_status NOT IN ('EXPIRED', 'INACTIVE')
	AND s.activation_method = 'FREE TRIAL'
	AND DATE(s.service_start_date) = DATE(GETDATE() - interval '1 days')
	AND s.account_id NOT IN (SELECT DISTINCT account_id FROM neon_reporting.test_accounts)
	ORDER BY account_id, dim_service_key
)

UNION

SELECT DISTINCT DATE(GETDATE() - interval '1 days') AS date
, 2 AS "ORDER"
, 'Convert Trial to Pay' AS measure
, COUNT(DISTINCT account_id) AS value
FROM
(
	SELECT DISTINCT s.account_id, s.dim_service_key, s.product_name, s.activation_method, s.service_status, s.service_start_date, s.service_end_date, s.coupon_code FROM dwh.dim_service s
	INNER JOIN dwh.dim_date d ON DATE(d.date_dt) >= DATE(s.service_start_date) AND DATE(d.date_dt) <= DATE(s.service_end_date)
	INNER JOIN paying p ON s.account_id = p.account_id
	WHERE d.date_dt = DATE(GETDATE() - interval '1 days')
	AND s.product_name IN ('Standard', 'Premium')
	AND s.service_status = 'EXPIRED'
	AND s.activation_method = 'FREE TRIAL'
	AND DATE(s.service_end_date) = DATE(GETDATE() - interval '1 days')
	AND s.account_id NOT IN (SELECT DISTINCT account_id FROM neon_reporting.test_accounts)
	ORDER BY account_id, dim_service_key
)

UNION

(

WITH sub AS
(
SELECT DISTINCT account_id, dim_service_key, s.product_name, activation_method, s.service_status, s.service_start_date, s.service_end_date, s.coupon_code FROM dwh.dim_service s
INNER JOIN dwh.dim_date d ON DATE(d.date_dt) >= DATE(s.service_start_date) AND DATE(d.date_dt) <= DATE(s.service_end_date)
WHERE d.date_dt = DATE(GETDATE() - interval '1 days')
AND s.product_name IN ('Standard', 'Premium')
AND s.service_status NOT IN ('INACTIVE','EXPIRED')
AND s.account_id NOT IN (SELECT DISTINCT account_id FROM neon_reporting.test_accounts)
ORDER BY account_id, dim_service_key
)

SELECT DISTINCT DATE(GETDATE() - interval '1 days') AS date
, 3 AS "ORDER"
, 'Winback' AS measure -- EXCLUDES DOWNGRADE FROM ANNUAL
, COUNT(DISTINCT account_id) AS value
FROM(
	SELECT DISTINCT account_id
	, dim_service_key
	, product_name
	, prior_activation_method
	, activation_method
	, prior_service_status
	, service_status
	, datediff('d', prior_service_end_date, service_start_date) AS days_ago
	, service_start_date
	, prior_service_end_date
	, service_end_date
	, coupon_code
	FROM
	(
		SELECT account_id, dim_service_key, s.product_name, activation_method, s.service_status, s.service_start_date, s.service_end_date, s.coupon_code
		, lag(s.activation_method) over (partition by dim_account_key order by dim_service_key) as prior_activation_method
		, lag(s.service_end_date) over (partition by dim_account_key order by dim_service_key) as prior_service_end_date
		, lag(s.service_status) over (partition by dim_account_key order by dim_service_key) as prior_service_status
		FROM dwh.dim_service s
		WHERE s.account_id IN (SELECT DISTINCT account_id FROM sub)
		AND s.product_name IN ('Standard', 'Premium')
		AND s.service_status NOT IN ('INACTIVE')
	)
	WHERE product_name IN ('Standard', 'Premium')
	AND service_status NOT IN ('INACTIVE','EXPIRED')
	AND activation_method iN ('PAYMENT', 'PAYMENT_VOUCHER', 'UNKNOWN')
	AND DATE(service_start_date) = DATE(GETDATE() - interval '1 days')
	AND days_ago > 0
)

)

UNION

SELECT DISTINCT DATE(GETDATE() - interval '1 days') AS date
, 6 AS "ORDER"
, 'Trial Ended' AS measure
, COUNT(DISTINCT account_id) AS value
FROM
(

	(
	SELECT DISTINCT s.account_id, s.dim_service_key, s.product_name, s.activation_method, s.service_status, s.service_start_date, s.service_end_date, s.coupon_code FROM dwh.dim_service s
	INNER JOIN dwh.dim_date d ON DATE(d.date_dt) >= DATE(s.service_start_date) AND DATE(d.date_dt) <= DATE(s.service_end_date)
	WHERE d.date_dt = DATE(GETDATE() - interval '1 days')
	AND s.product_name IN ('Standard', 'Premium')
	AND s.service_status = 'EXPIRED'
	AND s.activation_method = 'FREE TRIAL'
	AND DATE(s.service_end_date) = DATE(GETDATE() - interval '1 days')
	AND s.account_id NOT IN (SELECT DISTINCT account_id FROM neon_reporting.test_accounts)
	ORDER BY account_id, dim_service_key
	)

	UNION

	(
	SELECT DISTINCT s.account_id, s.dim_service_key, s.product_name, s.activation_method, s.service_status, s.service_start_date, s.service_end_date, s.coupon_code FROM dwh.dim_service s
	INNER JOIN dwh.dim_date d ON DATE(d.date_dt) >= DATE(s.service_start_date) AND DATE(d.date_dt) <= DATE(s.service_end_date)
	WHERE d.date_dt = DATE(GETDATE() - interval '1 days')
	AND s.product_name IN ('Standard', 'Premium')
	AND s.service_status = 'FINAL BILL'
	AND s.activation_method = 'FREE TRIAL'
	AND DATE(s.service_end_date) = DATE(GETDATE() - interval '1 days')
	AND s.account_id NOT IN (SELECT DISTINCT account_id FROM neon_reporting.test_accounts)
	ORDER BY account_id, dim_service_key
	)

)

UNION

SELECT DISTINCT DATE(GETDATE() - interval '1 days') AS date
, 4 AS "ORDER"
, 'Churned Trialists' AS measure --INCLUDES TRIAL TO ANNUAL
, COUNT(DISTINCT account_id) AS value
FROM
(
	SELECT DISTINCT s.account_id, s.dim_service_key, s.product_name, s.activation_method, s.service_status, s.service_start_date, s.service_end_date, s.coupon_code FROM dwh.dim_service s
	INNER JOIN dwh.dim_date d ON DATE(d.date_dt) >= DATE(s.service_start_date) AND DATE(d.date_dt) <= DATE(s.service_end_date)
	WHERE d.date_dt = DATE(GETDATE() - interval '1 days')
	AND s.product_name IN ('Standard', 'Premium')
	AND s.service_status = 'FINAL BILL'
	AND s.activation_method = 'FREE TRIAL'
	AND DATE(s.service_end_date) = DATE(GETDATE() - interval '1 days')
	AND s.account_id NOT IN (SELECT DISTINCT account_id FROM neon_reporting.test_accounts)
	ORDER BY account_id, dim_service_key
)

UNION

SELECT DISTINCT DATE(GETDATE() - interval '1 days') AS date
, 5 AS "ORDER"
, 'Paying Churn' AS measure --INCLUDES MONTHLY TO ANNUAL
, COUNT(DISTINCT account_id) AS value
FROM(
	SELECT DISTINCT account_id, dim_service_key, s.product_name, activation_method, s.service_status, s.service_start_date, s.service_end_date, s.coupon_code FROM dwh.dim_service s
	INNER JOIN dwh.dim_date d ON DATE(d.date_dt) >= DATE(s.service_start_date) AND DATE(d.date_dt) <= DATE(s.service_end_date)
	WHERE d.date_dt = DATE(GETDATE() - interval '1 days')
	AND s.product_name IN ('Standard', 'Premium')
	AND s.service_status IN ('FINAL BILL')
	AND DATE(s.service_end_date) = DATE(GETDATE() - interval '1 days')
	AND s.activation_method NOT IN ('FREE TRIAL', 'VOUCHER')
	AND s.account_id NOT IN (SELECT DISTINCT account_id FROM neon_reporting.test_accounts)
	AND s.account_id NOT IN (SELECT DISTINCT account_id FROM vip)
	ORDER BY account_id, dim_service_key
)

UNION

SELECT DISTINCT DATE(GETDATE() - interval '1 days') AS date
, 7 AS "ORDER"
, 'New DCB' AS measure
, COUNT(DISTINCT account_id) AS value
FROM
(
	SELECT DISTINCT account_id, dim_service_key, s.product_name, activation_method, s.service_status, s.service_start_date, s.service_end_date, s.coupon_code
	FROM dwh.dim_service s
	WHERE s.coupon_code LIKE 'SparkDCB%'
	--AND s.service_status IN ('ACTIVE', 'FINAL BILL')
	AND DATE(s.service_commence_date) = DATE(GETDATE() - interval '1 days')
	AND s.account_id NOT IN (SELECT DISTINCT account_id FROM neon_reporting.test_accounts)
)

UNION

SELECT DISTINCT DATE(GETDATE() - interval '1 days') AS date
, 8 AS "ORDER"
, 'DCB Churn' AS measure
, CASE WHEN COUNT(DISTINCT account_id) IS NULL THEN 0 ELSE COUNT(DISTINCT account_id) END AS value
FROM
(
	SELECT DISTINCT account_id, dim_service_key, s.product_name, activation_method, s.service_status, s.service_start_date, s.service_end_date, s.coupon_code
	FROM dwh.dim_service s
	WHERE s.coupon_code LIKE 'SparkDCB%'
	AND s.service_status IN ('FINAL BILL')
	AND DATE(s.service_end_date) = DATE(GETDATE() - interval '1 days')
	AND s.account_id NOT IN (SELECT DISTINCT account_id FROM neon_reporting.test_accounts)
)

UNION

SELECT DISTINCT DATE(GETDATE() - interval '1 days') AS date
, 9 AS "ORDER"
, 'DCB Pending Churn' AS measure
, CASE WHEN COUNT(DISTINCT account_id) IS NULL THEN 0 ELSE COUNT(DISTINCT account_id) END AS value
FROM
(
	SELECT DISTINCT account_id, dim_service_key, s.product_name, activation_method, s.service_status, s.service_start_date, s.service_end_date, s.coupon_code
	FROM dwh.dim_service s
	WHERE s.coupon_code LIKE 'SparkDCB%'
	AND s.service_status IN ('FINAL BILL')
	AND DATE(s.service_end_date) > DATE(GETDATE() - interval '1 days')
	AND s.account_id NOT IN (SELECT DISTINCT account_id FROM neon_reporting.test_accounts)
)

UNION

SELECT DISTINCT DATE(GETDATE() - interval '1 days') AS date
, 10 AS "ORDER"
, 'Closing Spark DCB' AS measure
, COUNT(DISTINCT account_id) AS value
FROM
(
	(
	SELECT DISTINCT account_id, dim_service_key, s.product_name, activation_method, s.service_status, s.service_start_date, s.service_end_date, s.coupon_code, s.evergent_order_id, s.evergent_ord_prod_id
	FROM dwh.dim_service s
	WHERE s.coupon_code LIKE 'SparkDCB%'
	AND s.service_status IN ('ACTIVE', 'FINAL BILL')
	AND DATE(GETDATE() - interval '1 days') BETWEEN DATE(s.service_start_date) AND DATE(s.service_end_date)
	AND s.account_id NOT IN (SELECT DISTINCT account_id FROM neon_reporting.test_accounts)
	)

		UNION

	(
	SELECT DISTINCT account_id, dim_service_key, s.product_name, activation_method, s.service_status, s.service_start_date, s.service_end_date, s.coupon_code, s.evergent_order_id, s.evergent_ord_prod_id
	FROM dwh.dim_service s
	WHERE s.coupon_code LIKE 'SparkDCB%'
	AND s.service_status IN ('ACTIVE', 'FINAL BILL')
	AND DATE(s.service_start_date) >= DATE(GETDATE())
	AND DATE(s.service_start_date) <= DATE(GETDATE() + interval '29 days')
	AND s.account_id NOT IN (SELECT DISTINCT account_id FROM neon_reporting.test_accounts)
	)
)

UNION

SELECT DISTINCT DATE(GETDATE() - interval '1 days') AS date
, 11 AS "ORDER"
, 'Closing Direct Trialist Base' AS measure
, COUNT(DISTINCT account_id) AS value
FROM
(
  	(
	SELECT DISTINCT account_id, dim_service_key, s.product_name, activation_method, s.service_status, s.service_start_date, s.service_end_date, s.coupon_code FROM dwh.dim_service s
	INNER JOIN dwh.dim_date d ON DATE(d.date_dt) >= DATE(s.service_start_date) AND DATE(d.date_dt) <= DATE(s.service_end_date)
	WHERE d.date_dt = DATE(GETDATE() - interval '1 days')
	AND s.product_name IN ('Standard', 'Premium')
	AND s.activation_method = 'FREE TRIAL'
	AND s.service_status NOT IN ('INACTIVE', 'EXPIRED')
	AND s.account_id NOT IN (SELECT DISTINCT account_id FROM neon_reporting.test_accounts)
	ORDER BY account_id, dim_service_key
    )
  	UNION
	
	(
	WITH min_date AS
	(
	SELECT DISTINCT s.dim_account_key, MIN(s.service_commence_date) as signup_date FROM dwh.dim_service s
	WHERE s.is_svod_flag = 'Y'
	GROUP BY 1
	)
		SELECT DISTINCT account_id, dim_service_key, product_name, activation_method, service_status, service_start_date, service_end_date, coupon_code
		FROM
		(
			SELECT s.*, a.first_name, a.last_name, a.email FROM dwh.dim_service s
			INNER JOIN min_date m ON s.dim_account_key = m.dim_account_key AND s.service_commence_date = m.signup_date
			INNER JOIN dwh.dim_account a ON s.dim_account_key = a.dim_account_key
			WHERE s.activation_method NOT IN ('FREE TRIAL', 'VOUCHER')
			AND s.is_svod_flag = 'Y'
			AND DATE(s.service_commence_date) BETWEEN DATE('2021-02-10') AND DATE('2021-02-16')
			AND DATE(s.service_commence_date) = DATE(s.service_start_date)
		)
		WHERE DATE(service_end_date) BETWEEN DATE(GETDATE()) AND DATE('2021-03-17')
	)
)

UNION

(
SELECT DISTINCT DATE(GETDATE() - interval '1 days') AS date
, 12 AS "ORDER"
, 'Closing Monthly Paying Base' AS measure --EXCLUDES ANNUAL PAYING BASE
, COUNT(DISTINCT account_id) AS value
FROM
(
	(
	SELECT DISTINCT s.account_id, s.activation_method, s.service_start_date, s.service_end_date
	FROM dwh.dim_service s
	INNER JOIN dwh.dim_account a ON s.dim_account_key = a.dim_account_key
	INNER JOIN dwh.dim_date d ON DATE(s.service_start_date) <= DATE(d.date_dt) AND DATE(s.service_end_date) >= DATE(d.date_dt)
	WHERE DATE(d.date_dt) = DATE(GETDATE() - interval '1 days')
	AND s.product_name IN ('Premium', 'Standard')
	AND s.service_status NOT IN ('INACTIVE','EXPIRED')
	AND activation_method NOT IN ('FREE TRIAL','VOUCHER')
	AND s.account_id NOT IN (SELECT DISTINCT account_id FROM neon_reporting.test_accounts)
	AND s.account_id NOT IN (SELECT DISTINCT account_id FROM neon_reporting.vip)
	ORDER BY 2,4
	)

	UNION

	(
	SELECT DISTINCT s.account_id, s.activation_method, s.service_start_date, s.service_end_date
	FROM dwh.dim_service s
	WHERE s.product_name IN ('Standard', 'Premium')
	AND s.service_status = 'ACTIVE'
	AND s.activation_method IN ('PAYMENT', 'PAYMENT_VOUCHER')
	AND DATE(s.service_start_date) >= DATE(GETDATE())
	AND DATE(s.service_start_date) <= DATE(GETDATE() + interval '29 days')
	AND s.account_id NOT IN (SELECT DISTINCT account_id FROM neon_reporting.test_accounts)
	--AND s.account_id NOT IN (SELECT DISTINCT account_id FROM vip)
	)
)
WHERE account_id NOT IN 
(

	WITH min_date AS
	(
	SELECT DISTINCT s.dim_account_key, MIN(s.service_commence_date) as signup_date FROM dwh.dim_service s
	WHERE s.is_svod_flag = 'Y'
	GROUP BY 1
	)

	SELECT DISTINCT account_id
	FROM
	(
		SELECT s.*, a.first_name, a.last_name, a.email FROM dwh.dim_service s
		INNER JOIN min_date m ON s.dim_account_key = m.dim_account_key AND s.service_commence_date = m.signup_date
		INNER JOIN dwh.dim_account a ON s.dim_account_key = a.dim_account_key
		WHERE s.activation_method NOT IN ('FREE TRIAL', 'VOUCHER')
		AND s.is_svod_flag = 'Y'
		AND DATE(s.service_commence_date) BETWEEN DATE('2021-02-10') AND DATE('2021-02-16')
		AND DATE(s.service_commence_date) = DATE(s.service_start_date)
	)
	WHERE DATE(service_end_date) BETWEEN DATE(GETDATE()) AND DATE('2021-03-17')

)
)

UNION

(
	SELECT DISTINCT DATE(GETDATE() - interval '1 days') AS date
	, 13 AS "ORDER"
	, 'Active DCB' AS measure
	, COUNT(DISTINCT account_id) AS value
	FROM
	(
		SELECT DISTINCT DATE(fe.start_timestamp), a.account_id FROM dwh.fact_engagement fe
		INNER JOIN dwh.dim_content c ON fe.dim_content_key = c.dim_content_key
		INNER JOIN dwh.dim_account a ON fe.dim_account_key = a.dim_account_key
		WHERE ((c.content_type = 'EPISODE') OR (c.content_type = 'FEATURE' AND lower(c.labels) LIKE '%neon%'))
		AND a.account_id IN
		(
			SELECT DISTINCT account_id
			FROM dwh.dim_service s
			WHERE s.coupon_code LIKE 'SparkDCB%'
			AND s.service_status IN ('ACTIVE', 'FINAL BILL')
			AND DATE(GETDATE() - interval '1 days') BETWEEN DATE(s.service_start_date) AND DATE(s.service_end_date)
			AND s.account_id NOT IN (SELECT DISTINCT account_id FROM neon_reporting.test_accounts)
		)
		AND DATE(fe.start_timestamp) = DATE(GETDATE() - interval '1 days')
	)
)

UNION

SELECT DISTINCT DATE(GETDATE() - interval '1 days') AS date
, 14 AS "ORDER"
, 'Suspended Accounts' AS measure --EXCLUDES SUSPENDED ANNUAL
, COUNT(DISTINCT account_id) AS value
FROM
(
	SELECT DISTINCT account_id, dim_service_key, s.product_name, activation_method, s.service_status, s.service_start_date, s.service_end_date, s.coupon_code
	FROM dwh.dim_service s
	INNER JOIN dwh.dim_date d ON DATE(d.date_dt) >= DATE(s.service_start_date) AND DATE(d.date_dt) <= DATE(s.service_end_date)
	WHERE s.product_name IN ('Standard', 'Premium')
	AND s.activation_method IN ('PAYMENT', 'PAYMENT_VOUCHER')
	AND s.service_status = 'HOLDONNPMT'
	AND DATE(s.service_end_date) = DATE(GETDATE())-2
	AND s.account_id NOT IN (SELECT DISTINCT account_id FROM neon_reporting.test_accounts)
	ORDER BY account_id, dim_service_key
)

UNION


(
WITH curr_dcb AS
(

	SELECT DISTINCT account_id, coupon_code, MIN(service_start_date) AS dcb_date
	FROM
	(
		(
		SELECT DISTINCT account_id, dim_service_key, s.product_name, activation_method, s.service_status, s.service_start_date, s.service_end_date, s.coupon_code, s.evergent_order_id, s.evergent_ord_prod_id
		FROM dwh.dim_service s
		WHERE s.coupon_code LIKE 'SparkDCB%'
		AND s.service_status IN ('ACTIVE', 'FINAL BILL')
		AND DATE(GETDATE() - interval '1 days') BETWEEN DATE(s.service_start_date) AND DATE(s.service_end_date)
		AND s.account_id NOT IN (SELECT DISTINCT account_id FROM neon_reporting.test_accounts)
		)

			UNION

		(
		SELECT DISTINCT account_id, dim_service_key, s.product_name, activation_method, s.service_status, s.service_start_date, s.service_end_date, s.coupon_code, s.evergent_order_id, s.evergent_ord_prod_id
		FROM dwh.dim_service s
		WHERE s.coupon_code LIKE 'SparkDCB%'
		AND s.service_status IN ('ACTIVE', 'FINAL BILL')
		AND DATE(s.service_start_date) >= DATE(GETDATE())
		AND DATE(s.service_start_date) <= DATE(GETDATE() + interval '29 days')
		AND s.account_id NOT IN (SELECT DISTINCT account_id FROM neon_reporting.test_accounts)
		)
	)
	GROUP BY 1,2

),

service AS
(
	WITH max_date AS
	(
	SELECT DISTINCT s.account_id, MAX(s.service_end_date) max_date FROM dwh.dim_service s
	INNER JOIN curr_dcb dcb ON s.account_id = dcb.account_id AND DATE(s.service_end_date) <= DATE(dcb_date)
	WHERE s.coupon_code NOT IN ('SparkDCBProd1')
	AND s.activation_method NOT IN ('INACTIVE')
	AND s.is_svod_flag = 'Y'
	GROUP BY 1
	)
	SELECT s.account_id, s.activation_method, s.service_status, s.service_start_date, s.service_end_date
	FROM dwh.dim_service s
	INNER JOIN max_date md ON s.account_id = md.account_id AND s.service_end_date = md.max_date
	WHERE s.coupon_code NOT IN ('SparkDCBProd1')
	AND s.is_svod_flag = 'Y'
	ORDER BY activation_method, service_end_date
),

activation AS
(
	WITH min_date AS
	(
	SELECT DISTINCT s.account_id, MIN(s.service_start_date) min_date FROM dwh.dim_service s
	INNER JOIN curr_dcb dcb ON s.account_id = dcb.account_id AND DATE(s.service_end_date) <= DATE(dcb_date)
	WHERE s.coupon_code NOT IN ('SparkDCBProd1')
	AND s.activation_method NOT IN ('INACTIVE')
	AND s.is_svod_flag = 'Y'
	GROUP BY 1
	)
	SELECT s.account_id, s.activation_method, s.service_status, s.service_start_date, s.service_end_date
	FROM dwh.dim_service s
	INNER JOIN min_date md ON s.account_id = md.account_id AND s.service_start_date = md.min_date
	WHERE s.coupon_code NOT IN ('SparkDCBProd1')
	AND s.is_svod_flag = 'Y'
	ORDER BY activation_method, service_end_date
)



SELECT DISTINCT DATE(dcb_date) AS date
, 15 AS "ORDER"
, 'Neon Covert to DCB' AS measure
, COUNT(DISTINCT account_id) AS value
FROM
(

SELECT dcb.*, s.activation_method, s.service_status, s.service_start_date, s.service_end_date
, datediff('day', s.service_start_date, s.service_end_date) AS service_period
, a.service_start_date AS activation_date
, datediff('day', s.service_end_date, dcb_date) AS gap
, datediff('day', activation_date, s.service_start_date) AS tenure
, CASE WHEN s.activation_method = 'FREE TRIAL' AND s.service_end_date < DATE('2020-07-31') AND service_period = 23 THEN 'Spark'
	WHEN s.activation_method = 'FREE TRIAL' AND service_period <= 0 THEN 'Spark'
	WHEN gap >30 THEN 'Spark'
	WHEN s.activation_method IS NULL THEN 'Spark'
	WHEN DATE(activation_date) = DATE(s.service_start_date) THEN 'Spark'
	WHEN tenure = 14 THEN 'Spark'
  END AS attribution
FROM curr_dcb dcb
LEFT JOIN service s ON dcb.account_id = s.account_id
LEFT JOIN activation a ON dcb.account_id = a.account_id
ORDER BY dcb_date, s.service_end_date

)
WHERE attribution IS NULL
AND date = DATE(GETDATE() - interval '1 days')
GROUP BY 1
ORDER BY 1
)

UNION

(
 
SELECT DISTINCT DATE(GETDATE() - interval '1 days') AS date
, 20 AS "ORDER"
, 'New Trialists L7D' AS measure
, COUNT(DISTINCT account_id) AS value
FROM
(
	SELECT DISTINCT s.account_id
  	FROM dwh.dim_service s	
  	INNER JOIN dwh.dim_date d ON DATE(d.date_dt) >= DATE(s.service_start_date) AND DATE(d.date_dt) <= DATE(s.service_end_date)
	WHERE d.date_dt = DATE(GETDATE())-1 
	AND s.product_name IN ('Standard', 'Premium')
	AND s.service_status NOT IN ('EXPIRED', 'INACTIVE')
	AND s.activation_method = 'FREE TRIAL'
	AND DATE(s.service_start_date) BETWEEN DATE(GETDATE())-7 AND DATE(GETDATE())-1 
	AND s.account_id NOT IN (SELECT DISTINCT account_id FROM neon_reporting.test_accounts)
	ORDER BY account_id
)
GROUP BY 1  
  
)


-----

UNION

SELECT DISTINCT DATE(GETDATE())-1 AS date
, 16 AS "order"
, 'Total New Annual' AS measure
, COUNT(DISTINCT account_id) AS value 
FROM
(
	SELECT DISTINCT account_id, dim_service_key, s.product_name, activation_method, s.service_status, s.service_start_date, s.service_end_date, s.coupon_code 
	FROM dwh.dim_service s
	INNER JOIN dwh.dim_date d ON DATE(d.date_dt) >= DATE(s.service_start_date) AND DATE(d.date_dt) <= DATE(s.service_end_date)
	WHERE s.product_name IN ('Annual')
	AND s.service_status NOT IN ('INACTIVE','EXPIRED')
	AND s.activation_method IN ('PAYMENT', 'PAYMENT_VOUCHER')
	AND DATE(s.service_commence_date) = DATE(GETDATE())-1
	AND s.account_id NOT IN (SELECT DISTINCT account_id FROM neon_reporting.test_accounts)
	ORDER BY account_id, dim_service_key
)

UNION

SELECT DISTINCT DATE(GETDATE())-1 AS date
, 26 AS "order"
, 'Closing Annual Base' AS measure
, COUNT(DISTINCT account_id) AS value 
FROM
(
	SELECT DISTINCT account_id, dim_service_key, s.product_name, activation_method, s.service_status, s.service_start_date, s.service_end_date, s.coupon_code 
	FROM dwh.dim_service s
	INNER JOIN dwh.dim_date d ON DATE(d.date_dt) >= DATE(s.service_start_date) AND DATE(d.date_dt) <= DATE(s.service_end_date)
	WHERE s.product_name IN ('Annual')
	AND s.activation_method IN ('PAYMENT', 'PAYMENT_VOUCHER')
	AND s.service_status NOT IN ('INACTIVE')
	AND DATE(d.date_dt) = DATE(GETDATE())-1
	AND s.account_id NOT IN (SELECT DISTINCT account_id FROM neon_reporting.test_accounts)
	ORDER BY account_id, dim_service_key
)

UNION

(

WITH sub AS
(
	SELECT DISTINCT account_id, dim_service_key, s.product_name, activation_method, s.service_status, s.service_start_date, s.service_end_date, s.coupon_code FROM dwh.dim_service s
	INNER JOIN dwh.dim_date d ON DATE(d.date_dt) >= DATE(s.service_start_date) AND DATE(d.date_dt) <= DATE(s.service_end_date)
	WHERE d.date_dt = DATE(GETDATE())-1
	AND s.product_name IN ('Standard','Premium','Annual')
	AND s.service_status NOT IN ('INACTIVE','EXPIRED')
	AND s.account_id NOT IN (SELECT DISTINCT account_id FROM neon_reporting.test_accounts)
	ORDER BY account_id, dim_service_key
)

SELECT DISTINCT DATE(GETDATE())-1 AS date
, 17 AS "order"
, 'New Annual Signup' AS measure
, COUNT(DISTINCT account_id) AS value 
FROM
(
	SELECT DISTINCT account_id
	, dim_service_key
	, product_name
	, prior_activation_method
	, activation_method
	, prior_service_status
	, service_status
	, datediff('d', prior_service_end_date, service_start_date) AS days_ago
	, service_start_date
	, prior_service_end_date
	, service_end_date
	, coupon_code
	FROM
	(
		SELECT account_id, dim_service_key, s.product_name, activation_method, s.service_status, s.service_start_date, s.service_end_date, s.coupon_code
		, lag(s.activation_method) over (partition by dim_account_key order by dim_service_key) as prior_activation_method
		, lag(s.service_end_date) over (partition by dim_account_key order by dim_service_key) as prior_service_end_date
		, lag(s.service_status) over (partition by dim_account_key order by dim_service_key) as prior_service_status
		FROM dwh.dim_service s
		WHERE s.account_id IN (SELECT DISTINCT account_id FROM sub)
		AND s.product_name IN ('Standard', 'Premium', 'Annual', 'Basic')
		AND s.service_status NOT IN ('INACTIVE')
	)
	WHERE product_name IN ('Annual')
	AND service_status NOT IN ('INACTIVE','EXPIRED')
	AND activation_method iN ('PAYMENT', 'PAYMENT_VOUCHER', 'UNKNOWN')
	AND DATE(service_start_date) = DATE(GETDATE())-1
	AND days_ago IS NULL
)

)

UNION

(

WITH sub AS
(
	SELECT DISTINCT account_id, dim_service_key, s.product_name, activation_method, s.service_status, s.service_start_date, s.service_end_date, s.coupon_code FROM dwh.dim_service s
	INNER JOIN dwh.dim_date d ON DATE(d.date_dt) >= DATE(s.service_start_date) AND DATE(d.date_dt) <= DATE(s.service_end_date)
	WHERE d.date_dt = DATE(GETDATE())-1
	AND s.product_name IN ('Standard','Premium','Annual', 'Basic')
	AND s.service_status NOT IN ('INACTIVE','EXPIRED')
	AND s.account_id NOT IN (SELECT DISTINCT account_id FROM neon_reporting.test_accounts)
	ORDER BY account_id, dim_service_key
)

SELECT DISTINCT DATE(GETDATE())-1 AS date
, 18 AS "order"
, 'Winback Annual' AS measure
, COUNT(DISTINCT account_id) AS value 
FROM
(
	SELECT DISTINCT account_id
	, dim_service_key
	, product_name
	, prior_activation_method
	, activation_method
	, prior_service_status
	, service_status
	, datediff('d', prior_service_end_date, service_start_date) AS days_ago
	, service_start_date
	, prior_service_end_date
	, service_end_date
	, coupon_code
	FROM
	(
		SELECT account_id, dim_service_key, s.product_name, activation_method, s.service_status, s.service_start_date, s.service_end_date, s.coupon_code
		, lag(s.activation_method) over (partition by dim_account_key order by dim_service_key) as prior_activation_method
		, lag(s.service_end_date) over (partition by dim_account_key order by dim_service_key) as prior_service_end_date
		, lag(s.service_status) over (partition by dim_account_key order by dim_service_key) as prior_service_status
		FROM dwh.dim_service s
		WHERE s.account_id IN (SELECT DISTINCT account_id FROM sub)
		AND s.product_name IN ('Standard', 'Premium', 'Annual', 'Basic')
		AND s.service_status NOT IN ('INACTIVE')
	)
	WHERE product_name IN ('Annual')
	AND service_status NOT IN ('INACTIVE','EXPIRED')
	AND activation_method iN ('PAYMENT', 'PAYMENT_VOUCHER', 'UNKNOWN')
	AND DATE(service_start_date) = DATE(GETDATE())-1
	AND days_ago > 0
)

)

UNION

(

WITH sub AS
(
	SELECT DISTINCT account_id, dim_service_key, s.product_name, activation_method, s.service_status, s.service_start_date, s.service_end_date, s.coupon_code FROM dwh.dim_service s
	INNER JOIN dwh.dim_date d ON DATE(d.date_dt) >= DATE(s.service_start_date) AND DATE(d.date_dt) <= DATE(s.service_end_date)
	WHERE d.date_dt = DATE(GETDATE())-1
	AND s.product_name IN ('Standard','Premium','Annual', 'Basic')
	AND s.service_status NOT IN ('INACTIVE','EXPIRED')
	AND s.account_id NOT IN (SELECT DISTINCT account_id FROM neon_reporting.test_accounts)
	ORDER BY account_id, dim_service_key
)

SELECT DISTINCT DATE(GETDATE())-1 AS date
, 19 AS "order"
, 'Upgrade to Annual' AS measure
, COUNT(DISTINCT account_id) AS value 
FROM
(
	SELECT DISTINCT account_id
	, dim_service_key
	, prior_product_name
	, product_name
	, prior_activation_method
	, activation_method
	, prior_service_status
	, service_status
	, datediff('d', prior_service_end_date, service_start_date) AS days_ago
	, service_start_date
	, prior_service_end_date
	, service_end_date
	, coupon_code
	FROM
	(
		SELECT account_id, dim_service_key, s.product_name, activation_method, s.service_status, s.service_start_date, s.service_end_date, s.coupon_code
		, lag(s.product_name) over (partition by dim_account_key order by dim_service_key) as prior_product_name
		, lag(s.activation_method) over (partition by dim_account_key order by dim_service_key) as prior_activation_method
		, lag(s.service_end_date) over (partition by dim_account_key order by dim_service_key) as prior_service_end_date
		, lag(s.service_status) over (partition by dim_account_key order by dim_service_key) as prior_service_status
		FROM dwh.dim_service s
		WHERE s.account_id IN (SELECT DISTINCT account_id FROM sub)
		AND s.product_name IN ('Standard', 'Premium', 'Annual', 'Basic')
		AND s.service_status NOT IN ('INACTIVE')
	)
	WHERE product_name IN ('Annual')
	AND service_status NOT IN ('INACTIVE','EXPIRED')
	AND activation_method iN ('PAYMENT', 'PAYMENT_VOUCHER', 'UNKNOWN')
	AND DATE(service_start_date) = DATE(GETDATE())-1
	AND days_ago = 0
	AND prior_product_name <> 'Annual'
)

)

UNION

(
SELECT DISTINCT DATE(GETDATE())-1 AS date
, 27 AS "order"
, 'Pending Annual' AS measure
, COUNT(DISTINCT s.dim_account_key) AS value
FROM dwh.dim_service s
WHERE s.product_name = 'Annual'
AND s.account_id NOT IN (SELECT DISTINCT account_id FROM neon_reporting.test_accounts)
AND DATE(s.service_start_date) >= DATE(GETDATE())
)

-----

UNION

(
 
SELECT DISTINCT DATE(GETDATE() - interval '1 days') AS date
, 21 AS "ORDER"
, 'Active Trialists L7D' AS measure
, COUNT(DISTINCT fe.dim_account_key) AS value
FROM dwh.fact_engagement fe
INNER JOIN dwh.dim_account a ON fe.dim_account_key = a.dim_account_key
INNER JOIN dwh.dim_content c ON fe.dim_content_key = c.dim_content_key
WHERE ((c.content_type = 'EPISODE') OR (c.content_type = 'FEATURE' AND lower(c.labels) LIKE '%neon%'))
AND DATE(fe.start_timestamp) BETWEEN DATE(GETDATE())-7 AND DATE(GETDATE())-1 
AND fe.play_seconds >= 15
AND a.account_id IN
(
	SELECT DISTINCT s.account_id
	FROM dwh.dim_service s	
	INNER JOIN dwh.dim_date d ON DATE(d.date_dt) >= DATE(s.service_start_date) AND DATE(d.date_dt) <= DATE(s.service_end_date)
	WHERE d.date_dt = DATE(GETDATE())-1 
	AND s.product_name IN ('Standard', 'Premium')
	AND s.service_status NOT IN ('EXPIRED', 'INACTIVE')
	AND s.activation_method = 'FREE TRIAL'
	AND DATE(s.service_start_date) BETWEEN DATE(GETDATE())-7 AND DATE(GETDATE())-1 
	AND s.account_id NOT IN (SELECT DISTINCT account_id FROM neon_reporting.test_accounts)
	ORDER BY account_id
)
GROUP BY 1

)

UNION

(
  
SELECT DISTINCT DATE(GETDATE() - interval '1 days') AS date
, 22 AS "ORDER"
, 'Active Direct L30D' AS measure
, COUNT(DISTINCT account_id) AS value
FROM dwh.fact_engagement fe
INNER JOIN dwh.dim_account a ON fe.dim_account_key = a.dim_account_key
INNER JOIN dwh.dim_content c ON fe.dim_content_key = c.dim_content_key
WHERE ((c.content_type = 'EPISODE') OR (c.content_type = 'FEATURE' AND lower(c.labels) LIKE '%neon%'))
AND DATE(fe.start_timestamp) BETWEEN DATE(GETDATE())-30 AND DATE(GETDATE())-1 
AND fe.play_seconds >= 15
AND a.account_id IN
(  
  	(
	SELECT DISTINCT s.account_id
	FROM dwh.dim_service s
	INNER JOIN dwh.dim_account a ON s.dim_account_key = a.dim_account_key
	INNER JOIN dwh.dim_date d ON DATE(s.service_start_date) <= DATE(d.date_dt) AND DATE(s.service_end_date) >= DATE(d.date_dt)
	WHERE DATE(d.date_dt) = DATE(GETDATE() - interval '1 days')
	AND s.product_name IN ('Premium', 'Standard')
	AND s.service_status NOT IN ('INACTIVE','EXPIRED')
	AND activation_method NOT IN ('FREE TRIAL','VOUCHER')
	AND s.account_id NOT IN (SELECT DISTINCT account_id FROM neon_reporting.test_accounts)
	AND s.account_id NOT IN (SELECT DISTINCT account_id FROM vip)
	)

	UNION

	(
	SELECT DISTINCT s.account_id
	FROM dwh.dim_service s
	WHERE s.product_name IN ('Standard', 'Premium')
	AND s.service_status = 'ACTIVE'
	AND s.activation_method IN ('PAYMENT', 'PAYMENT_VOUCHER')
	AND DATE(s.service_start_date) >= DATE(GETDATE())
	AND DATE(s.service_start_date) <= DATE(GETDATE() + interval '29 days')
	AND s.account_id NOT IN (SELECT DISTINCT account_id FROM neon_reporting.test_accounts)
	--AND s.account_id NOT IN (SELECT DISTINCT account_id FROM vip)
	)
)
GROUP BY 1
  
)

UNION

(

SELECT DISTINCT DATE(GETDATE() - interval '1 days') AS date
, 23 AS "ORDER"
, 'Active DCB L30D' AS measure
, COUNT(DISTINCT account_id) AS value
FROM dwh.fact_engagement fe
INNER JOIN dwh.dim_account a ON fe.dim_account_key = a.dim_account_key
INNER JOIN dwh.dim_content c ON fe.dim_content_key = c.dim_content_key
WHERE ((c.content_type = 'EPISODE') OR (c.content_type = 'FEATURE' AND lower(c.labels) LIKE '%neon%'))
AND DATE(fe.start_timestamp) BETWEEN DATE(GETDATE())-30 AND DATE(GETDATE())-1
AND fe.play_seconds >= 15
AND a.account_id IN
(
	(
	SELECT DISTINCT account_id
	FROM dwh.dim_service s
	WHERE s.coupon_code LIKE 'SparkDCB%'
	AND s.service_status IN ('ACTIVE', 'FINAL BILL')
	AND DATE(GETDATE())-1 BETWEEN DATE(s.service_start_date) AND DATE(s.service_end_date)
	AND s.account_id NOT IN (SELECT DISTINCT account_id FROM neon_reporting.test_accounts)
	)
	
	UNION 
	
	(
	SELECT DISTINCT account_id
	FROM dwh.dim_service s
	WHERE s.coupon_code LIKE 'SparkDCB%'
	AND s.service_status IN ('ACTIVE', 'FINAL BILL')
	AND DATE(s.service_start_date) >= DATE(GETDATE())
	AND DATE(s.service_start_date) <= DATE(GETDATE())+29
	AND s.account_id NOT IN (SELECT DISTINCT account_id FROM neon_reporting.test_accounts)
	)
	
)
GROUP BY 1
  
)

UNION

(

SELECT DISTINCT DATE(GETDATE() - interval '1 days') AS date
, 24 AS "ORDER"
, 'Total Hours' AS measure
, SUM(hours_watched) AS value 
FROM
(
SELECT DISTINCT DATE(fe.start_timestamp), SUM(fe.play_seconds)/3600 AS hours_watched
FROM dwh.fact_engagement fe
INNER JOIN dwh.dim_content c ON fe.dim_content_key = c.dim_content_key
WHERE ((c.content_type = 'EPISODE') OR (c.content_type = 'FEATURE' AND c.labels ilike '%neon%' ))
AND DATE(fe.start_timestamp) >= DATE(GETDATE())-1
GROUP BY 1
)
GROUP BY 1  
  
)

UNION

(
  
SELECT DISTINCT DATE(GETDATE() - interval '1 days') AS date
, 25 AS "ORDER"
, 'Unique Viewers' AS measure
, COUNT(DISTINCT dim_account_key) AS value FROM
(
SELECT DISTINCT DATE(fe.start_timestamp), fe.dim_account_key
FROM dwh.fact_engagement fe
INNER JOIN dwh.dim_content c ON fe.dim_content_key = c.dim_content_key
WHERE ((c.content_type = 'EPISODE') OR (c.content_type = 'FEATURE' AND c.labels ilike '%neon%' ))
AND DATE(fe.start_timestamp) >= DATE(GETDATE())-1
)
GROUP BY 1  
  
)

UNION

(
  
SELECT DISTINCT DATE(GETDATE() - interval '1 days') AS date
, 28 AS "ORDER"
, 'New Annual Free Trialists' AS measure
, COUNT(DISTINCT account_id) AS value
FROM
(
	SELECT DISTINCT s.account_id, s.dim_service_key, s.product_name, s.activation_method, s.service_status, s.service_start_date, s.service_end_date, s.coupon_code FROM dwh.dim_service s
	INNER JOIN dwh.dim_date d ON DATE(d.date_dt) >= DATE(s.service_start_date) AND DATE(d.date_dt) <= DATE(s.service_end_date)
	WHERE d.date_dt = DATE(GETDATE() - interval '1 days')
	AND s.product_name IN ('Annual')
	AND s.service_status NOT IN ('EXPIRED', 'INACTIVE')
	AND s.activation_method = 'FREE TRIAL'
	AND DATE(s.service_start_date) = DATE(GETDATE() - interval '1 days')
	AND s.account_id NOT IN (SELECT DISTINCT account_id FROM neon_reporting.test_accounts)
	ORDER BY account_id, dim_service_key
)
  
)

UNION

(
  
SELECT DISTINCT DATE(GETDATE() - interval '1 days') AS date
, 29 AS "ORDER"
, 'Closing Annual Trialist Base' AS measure
, COUNT(DISTINCT account_id) AS value
FROM
(
	SELECT DISTINCT account_id, dim_service_key, s.product_name, activation_method, s.service_status, s.service_start_date, s.service_end_date, s.coupon_code FROM dwh.dim_service s
	INNER JOIN dwh.dim_date d ON DATE(d.date_dt) >= DATE(s.service_start_date) AND DATE(d.date_dt) <= DATE(s.service_end_date)
	WHERE d.date_dt = DATE(GETDATE() - interval '1 days')
	AND s.product_name IN ('Annual')
	AND s.activation_method = 'FREE TRIAL'
	AND s.service_status NOT IN ('INACTIVE', 'EXPIRED')
	AND s.account_id NOT IN (SELECT DISTINCT account_id FROM neon_reporting.test_accounts)
	ORDER BY account_id, dim_service_key

)
)
  
UNION

(
SELECT DISTINCT DATE(GETDATE() - interval '1 days') AS date
, 30 AS "ORDER"
, 'Convert Annual Trial to Pay' AS measure
, COUNT(DISTINCT account_id) AS value
FROM
(
	SELECT DISTINCT s.account_id, s.dim_service_key, s.product_name, s.activation_method, s.service_status, s.service_start_date, s.service_end_date, s.coupon_code FROM dwh.dim_service s
	INNER JOIN dwh.dim_date d ON DATE(d.date_dt) >= DATE(s.service_start_date) AND DATE(d.date_dt) <= DATE(s.service_end_date)
	INNER JOIN paying p ON s.account_id = p.account_id
	WHERE d.date_dt = DATE(GETDATE() - interval '1 days')
	AND s.product_name IN ('Annual')
	AND s.service_status = 'EXPIRED'
	AND s.activation_method = 'FREE TRIAL'
	AND DATE(s.service_end_date) = DATE(GETDATE() - interval '1 days')
	AND s.account_id NOT IN (SELECT DISTINCT account_id FROM neon_reporting.test_accounts)
	ORDER BY account_id, dim_service_key
)  
)

UNION

(
SELECT DISTINCT DATE(GETDATE() - interval '1 days') AS date
, 31 AS "ORDER"
, 'Annual Trial Ended' AS measure
, COUNT(DISTINCT account_id) AS value
FROM
(

	(
	SELECT DISTINCT s.account_id, s.dim_service_key, s.product_name, s.activation_method, s.service_status, s.service_start_date, s.service_end_date, s.coupon_code FROM dwh.dim_service s
	INNER JOIN dwh.dim_date d ON DATE(d.date_dt) >= DATE(s.service_start_date) AND DATE(d.date_dt) <= DATE(s.service_end_date)
	WHERE d.date_dt = DATE(GETDATE() - interval '1 days')
	AND s.product_name IN ('Annual')
	AND s.service_status = 'EXPIRED'
	AND s.activation_method = 'FREE TRIAL'
	AND DATE(s.service_end_date) = DATE(GETDATE() - interval '1 days')
	AND s.account_id NOT IN (SELECT DISTINCT account_id FROM neon_reporting.test_accounts)
	ORDER BY account_id, dim_service_key
	)

	UNION

	(
	SELECT DISTINCT s.account_id, s.dim_service_key, s.product_name, s.activation_method, s.service_status, s.service_start_date, s.service_end_date, s.coupon_code FROM dwh.dim_service s
	INNER JOIN dwh.dim_date d ON DATE(d.date_dt) >= DATE(s.service_start_date) AND DATE(d.date_dt) <= DATE(s.service_end_date)
	WHERE d.date_dt = DATE(GETDATE() - interval '1 days')
	AND s.product_name IN ('Annual')
	AND s.service_status = 'FINAL BILL'
	AND s.activation_method = 'FREE TRIAL'
	AND DATE(s.service_end_date) = DATE(GETDATE() - interval '1 days')
	AND s.account_id NOT IN (SELECT DISTINCT account_id FROM neon_reporting.test_accounts)
	ORDER BY account_id, dim_service_key
	)
)
  
)

UNION

(
  
SELECT DISTINCT DATE(GETDATE() - interval '1 days') AS date
, 32 AS "ORDER"
, 'Active Annual L30D' AS measure
, COUNT(DISTINCT account_id) AS value
FROM dwh.fact_engagement fe
INNER JOIN dwh.dim_account a ON fe.dim_account_key = a.dim_account_key
INNER JOIN dwh.dim_content c ON fe.dim_content_key = c.dim_content_key
WHERE ((c.content_type = 'EPISODE') OR (c.content_type = 'FEATURE' AND lower(c.labels) LIKE '%neon%'))
AND DATE(fe.start_timestamp) BETWEEN DATE(GETDATE())-30 AND DATE(GETDATE())-1 
AND fe.play_seconds >= 15
AND a.account_id IN
(  
	SELECT DISTINCT account_id
	FROM dwh.dim_service s
	INNER JOIN dwh.dim_date d ON DATE(d.date_dt) >= DATE(s.service_start_date) AND DATE(d.date_dt) <= DATE(s.service_end_date)
	WHERE s.product_name IN ('Annual')
	AND s.activation_method IN ('PAYMENT')
	AND s.service_status NOT IN ('INACTIVE')
	AND DATE(d.date_dt) = DATE(GETDATE())-1
	AND s.account_id NOT IN (SELECT DISTINCT account_id FROM neon_reporting.test_accounts)
	ORDER BY account_id
)
GROUP BY 1
  
UNION

(
  
SELECT DISTINCT DATE(GETDATE() - interval '1 days') AS date
, 33 AS "ORDER"
, 'New Basic Free Trialists' AS measure
, COUNT(DISTINCT account_id) AS value
FROM
(
	SELECT DISTINCT s.account_id, s.dim_service_key, s.product_name, s.activation_method, s.service_status, s.service_start_date, s.service_end_date, s.coupon_code FROM dwh.dim_service s
	INNER JOIN dwh.dim_date d ON DATE(d.date_dt) >= DATE(s.service_start_date) AND DATE(d.date_dt) <= DATE(s.service_end_date)
	WHERE d.date_dt = DATE(GETDATE() - interval '1 days')
	AND s.product_name IN ('Basic')
	AND s.service_status NOT IN ('EXPIRED', 'INACTIVE')
	AND s.activation_method = 'FREE TRIAL'
	AND DATE(s.service_start_date) = DATE(GETDATE() - interval '1 days')
	AND s.account_id NOT IN (SELECT DISTINCT account_id FROM neon_reporting.test_accounts)
	ORDER BY account_id, dim_service_key
)
  
)

UNION

(
  
SELECT DISTINCT DATE(GETDATE() - interval '1 days') AS date
, 34 AS "ORDER"
, 'Closing Basic Trialist Base' AS measure
, COUNT(DISTINCT account_id) AS value
FROM
(
	SELECT DISTINCT account_id, dim_service_key, s.product_name, activation_method, s.service_status, s.service_start_date, s.service_end_date, s.coupon_code FROM dwh.dim_service s
	INNER JOIN dwh.dim_date d ON DATE(d.date_dt) >= DATE(s.service_start_date) AND DATE(d.date_dt) <= DATE(s.service_end_date)
	WHERE d.date_dt = DATE(GETDATE() - interval '1 days')
	AND s.product_name IN ('Basic')
	AND s.activation_method = 'FREE TRIAL'
	AND s.service_status NOT IN ('INACTIVE', 'EXPIRED')
	AND s.account_id NOT IN (SELECT DISTINCT account_id FROM neon_reporting.test_accounts)
	ORDER BY account_id, dim_service_key

)
)
  
UNION

(
SELECT DISTINCT DATE(GETDATE() - interval '1 days') AS date
, 35 AS "ORDER"
, 'Convert Basic Trial to Pay' AS measure
, COUNT(DISTINCT account_id) AS value
FROM
(
	SELECT DISTINCT s.account_id, s.dim_service_key, s.product_name, s.activation_method, s.service_status, s.service_start_date, s.service_end_date, s.coupon_code FROM dwh.dim_service s
	INNER JOIN dwh.dim_date d ON DATE(d.date_dt) >= DATE(s.service_start_date) AND DATE(d.date_dt) <= DATE(s.service_end_date)
	INNER JOIN paying p ON s.account_id = p.account_id
	WHERE d.date_dt = DATE(GETDATE() - interval '1 days')
	AND s.product_name IN ('Basic')
	AND s.service_status = 'EXPIRED'
	AND s.activation_method = 'FREE TRIAL'
	AND DATE(s.service_end_date) = DATE(GETDATE() - interval '1 days')
	AND s.account_id NOT IN (SELECT DISTINCT account_id FROM neon_reporting.test_accounts)
	ORDER BY account_id, dim_service_key
)  
)

UNION

(
SELECT DISTINCT DATE(GETDATE() - interval '1 days') AS date
, 36 AS "ORDER"
, 'Basic Trial Ended' AS measure
, COUNT(DISTINCT account_id) AS value
FROM
(

	(
	SELECT DISTINCT s.account_id, s.dim_service_key, s.product_name, s.activation_method, s.service_status, s.service_start_date, s.service_end_date, s.coupon_code FROM dwh.dim_service s
	INNER JOIN dwh.dim_date d ON DATE(d.date_dt) >= DATE(s.service_start_date) AND DATE(d.date_dt) <= DATE(s.service_end_date)
	WHERE d.date_dt = DATE(GETDATE() - interval '1 days')
	AND s.product_name IN ('Basic')
	AND s.service_status = 'EXPIRED'
	AND s.activation_method = 'FREE TRIAL'
	AND DATE(s.service_end_date) = DATE(GETDATE() - interval '1 days')
	AND s.account_id NOT IN (SELECT DISTINCT account_id FROM neon_reporting.test_accounts)
	ORDER BY account_id, dim_service_key
	)

	UNION

	(
	SELECT DISTINCT s.account_id, s.dim_service_key, s.product_name, s.activation_method, s.service_status, s.service_start_date, s.service_end_date, s.coupon_code FROM dwh.dim_service s
	INNER JOIN dwh.dim_date d ON DATE(d.date_dt) >= DATE(s.service_start_date) AND DATE(d.date_dt) <= DATE(s.service_end_date)
	WHERE d.date_dt = DATE(GETDATE() - interval '1 days')
	AND s.product_name IN ('Basic')
	AND s.service_status = 'FINAL BILL'
	AND s.activation_method = 'FREE TRIAL'
	AND DATE(s.service_end_date) = DATE(GETDATE() - interval '1 days')
	AND s.account_id NOT IN (SELECT DISTINCT account_id FROM neon_reporting.test_accounts)
	ORDER BY account_id, dim_service_key
	)
)
  
)

UNION

(
  
SELECT DISTINCT DATE(GETDATE() - interval '1 days') AS date
, 37 AS "ORDER"
, 'Active Basic L30D' AS measure
, COUNT(DISTINCT account_id) AS value
FROM dwh.fact_engagement fe
INNER JOIN dwh.dim_account a ON fe.dim_account_key = a.dim_account_key
INNER JOIN dwh.dim_content c ON fe.dim_content_key = c.dim_content_key
WHERE ((c.content_type = 'EPISODE') OR (c.content_type = 'FEATURE' AND lower(c.labels) LIKE '%neon%'))
AND DATE(fe.start_timestamp) BETWEEN DATE(GETDATE())-30 AND DATE(GETDATE())-1 
AND fe.play_seconds >= 15
AND a.account_id IN
(  
	SELECT DISTINCT account_id
	FROM dwh.dim_service s
	INNER JOIN dwh.dim_date d ON DATE(d.date_dt) >= DATE(s.service_start_date) AND DATE(d.date_dt) <= DATE(s.service_end_date)
	WHERE s.product_name IN ('Basic')
	AND s.activation_method IN ('PAYMENT', 'PAYMENT_VOUCHER')
	AND s.service_status NOT IN ('INACTIVE')
	AND DATE(d.date_dt) = DATE(GETDATE())-1
	AND s.account_id NOT IN (SELECT DISTINCT account_id FROM neon_reporting.test_accounts)
	ORDER BY account_id
)
GROUP BY 1
))
  
UNION

SELECT DISTINCT DATE(GETDATE())-1 AS date
, 38 AS "order"
, 'Total New Basic' AS measure
, COUNT(DISTINCT account_id) AS value 
FROM
(
	SELECT DISTINCT account_id, dim_service_key, s.product_name, activation_method, s.service_status, s.service_start_date, s.service_end_date, s.coupon_code 
	FROM dwh.dim_service s
	INNER JOIN dwh.dim_date d ON DATE(d.date_dt) >= DATE(s.service_start_date) AND DATE(d.date_dt) <= DATE(s.service_end_date)
	WHERE s.product_name IN ('Basic')
	AND s.service_status NOT IN ('INACTIVE','EXPIRED')
	AND s.activation_method IN ('PAYMENT', 'PAYMENT_VOUCHER')
	AND DATE(s.service_commence_date) = DATE(GETDATE())-1
	AND s.account_id NOT IN (SELECT DISTINCT account_id FROM neon_reporting.test_accounts)
	ORDER BY account_id, dim_service_key
)

UNION

SELECT DISTINCT DATE(GETDATE())-1 AS date
, 39 AS "order"
, 'Closing Basic Base' AS measure
, COUNT(DISTINCT account_id) AS value 
FROM
(
	SELECT DISTINCT account_id, dim_service_key, s.product_name, activation_method, s.service_status, s.service_start_date, s.service_end_date, s.coupon_code 
	FROM dwh.dim_service s
	INNER JOIN dwh.dim_date d ON DATE(d.date_dt) >= DATE(s.service_start_date) AND DATE(d.date_dt) <= DATE(s.service_end_date)
	WHERE s.product_name IN ('Basic')
	AND s.activation_method IN ('PAYMENT', 'PAYMENT_VOUCHER')
	AND s.service_status NOT IN ('INACTIVE')
	AND DATE(d.date_dt) = DATE(GETDATE())-1
	AND s.account_id NOT IN (SELECT DISTINCT account_id FROM neon_reporting.test_accounts)
	ORDER BY account_id, dim_service_key
)

UNION

(

WITH sub AS
(
	SELECT DISTINCT account_id, dim_service_key, s.product_name, activation_method, s.service_status, s.service_start_date, s.service_end_date, s.coupon_code FROM dwh.dim_service s
	INNER JOIN dwh.dim_date d ON DATE(d.date_dt) >= DATE(s.service_start_date) AND DATE(d.date_dt) <= DATE(s.service_end_date)
	WHERE d.date_dt = DATE(GETDATE())-1
	AND s.product_name IN ('Standard','Premium','Annual', 'Basic')
	AND s.service_status NOT IN ('INACTIVE','EXPIRED')
	AND s.account_id NOT IN (SELECT DISTINCT account_id FROM neon_reporting.test_accounts)
	ORDER BY account_id, dim_service_key
)

SELECT DISTINCT DATE(GETDATE())-1 AS date
, 40 AS "order"
, 'New Basic Signup' AS measure
, COUNT(DISTINCT account_id) AS value 
FROM
(
	SELECT DISTINCT account_id
	, dim_service_key
	, product_name
	, prior_activation_method
	, activation_method
	, prior_service_status
	, service_status
	, datediff('d', prior_service_end_date, service_start_date) AS days_ago
	, service_start_date
	, prior_service_end_date
	, service_end_date
	, coupon_code
	FROM
	(
		SELECT account_id, dim_service_key, s.product_name, activation_method, s.service_status, s.service_start_date, s.service_end_date, s.coupon_code
		, lag(s.activation_method) over (partition by dim_account_key order by dim_service_key) as prior_activation_method
		, lag(s.service_end_date) over (partition by dim_account_key order by dim_service_key) as prior_service_end_date
		, lag(s.service_status) over (partition by dim_account_key order by dim_service_key) as prior_service_status
		FROM dwh.dim_service s
		WHERE s.account_id IN (SELECT DISTINCT account_id FROM sub)
		AND s.product_name IN ('Standard', 'Premium', 'Annual', 'Basic')
		AND s.service_status NOT IN ('INACTIVE')
	)
	WHERE product_name IN ('Basic')
	AND service_status NOT IN ('INACTIVE','EXPIRED')
	AND activation_method iN ('PAYMENT', 'PAYMENT_VOUCHER', 'UNKNOWN')
	AND DATE(service_start_date) = DATE(GETDATE())-1
	AND days_ago IS NULL
)

)

UNION

(

WITH sub AS
(
	SELECT DISTINCT account_id, dim_service_key, s.product_name, activation_method, s.service_status, s.service_start_date, s.service_end_date, s.coupon_code FROM dwh.dim_service s
	INNER JOIN dwh.dim_date d ON DATE(d.date_dt) >= DATE(s.service_start_date) AND DATE(d.date_dt) <= DATE(s.service_end_date)
	WHERE d.date_dt = DATE(GETDATE())-1
	AND s.product_name IN ('Standard','Premium','Annual', 'Basic')
	AND s.service_status NOT IN ('INACTIVE','EXPIRED')
	AND s.account_id NOT IN (SELECT DISTINCT account_id FROM neon_reporting.test_accounts)
	ORDER BY account_id, dim_service_key
)

SELECT DISTINCT DATE(GETDATE())-1 AS date
, 41 AS "order"
, 'Winback Basic' AS measure
, COUNT(DISTINCT account_id) AS value 
FROM
(
	SELECT DISTINCT account_id
	, dim_service_key
	, product_name
	, prior_activation_method
	, activation_method
	, prior_service_status
	, service_status
	, datediff('d', prior_service_end_date, service_start_date) AS days_ago
	, service_start_date
	, prior_service_end_date
	, service_end_date
	, coupon_code
	FROM
	(
		SELECT account_id, dim_service_key, s.product_name, activation_method, s.service_status, s.service_start_date, s.service_end_date, s.coupon_code
		, lag(s.activation_method) over (partition by dim_account_key order by dim_service_key) as prior_activation_method
		, lag(s.service_end_date) over (partition by dim_account_key order by dim_service_key) as prior_service_end_date
		, lag(s.service_status) over (partition by dim_account_key order by dim_service_key) as prior_service_status
		FROM dwh.dim_service s
		WHERE s.account_id IN (SELECT DISTINCT account_id FROM sub)
		AND s.product_name IN ('Standard', 'Premium', 'Annual', 'Basic')
		AND s.service_status NOT IN ('INACTIVE')
	)
	WHERE product_name IN ('Basic')
	AND service_status NOT IN ('INACTIVE','EXPIRED')
	AND activation_method iN ('PAYMENT', 'PAYMENT_VOUCHER', 'UNKNOWN')
	AND DATE(service_start_date) = DATE(GETDATE())-1
	AND days_ago > 0
))


UNION

(

WITH sub AS
(
	SELECT DISTINCT account_id, dim_service_key, s.product_name, activation_method, s.service_status, s.service_start_date, s.service_end_date, s.coupon_code FROM dwh.dim_service s
	INNER JOIN dwh.dim_date d ON DATE(d.date_dt) >= DATE(s.service_start_date) AND DATE(d.date_dt) <= DATE(s.service_end_date)
	WHERE d.date_dt = DATE(GETDATE())-1
	AND s.product_name IN ('Standard','Premium','Annual', 'Basic')
	AND s.service_status NOT IN ('INACTIVE','EXPIRED')
	AND s.account_id NOT IN (SELECT DISTINCT account_id FROM neon_reporting.test_accounts)
	ORDER BY account_id, dim_service_key
)

SELECT DISTINCT DATE(GETDATE())-1 AS date
, 42 AS "order"
, 'Downgrade to Basic' AS measure
, COUNT(DISTINCT account_id) AS value 
FROM
(
	SELECT DISTINCT account_id
	, dim_service_key
	, prior_product_name
	, product_name
	, prior_activation_method
	, activation_method
	, prior_service_status
	, service_status
	, datediff('d', prior_service_end_date, service_start_date) AS days_ago
	, service_start_date
	, prior_service_end_date
	, service_end_date
	, coupon_code
	FROM
	(
		SELECT account_id, dim_service_key, s.product_name, activation_method, s.service_status, s.service_start_date, s.service_end_date, s.coupon_code
		, lag(s.product_name) over (partition by dim_account_key order by dim_service_key) as prior_product_name
		, lag(s.activation_method) over (partition by dim_account_key order by dim_service_key) as prior_activation_method
		, lag(s.service_end_date) over (partition by dim_account_key order by dim_service_key) as prior_service_end_date
		, lag(s.service_status) over (partition by dim_account_key order by dim_service_key) as prior_service_status
		FROM dwh.dim_service s
		WHERE s.account_id IN (SELECT DISTINCT account_id FROM sub)
		AND s.product_name IN ('Standard', 'Premium', 'Annual', 'Basic')
		AND s.service_status NOT IN ('INACTIVE')
	)
	WHERE product_name IN ('Basic')
	AND service_status NOT IN ('INACTIVE','EXPIRED')
	AND activation_method iN ('PAYMENT', 'PAYMENT_VOUCHER', 'UNKNOWN')
	AND DATE(service_start_date) = DATE(GETDATE())-1
	AND days_ago = 0
	AND prior_product_name <> 'Basic'
))


UNION

SELECT DISTINCT DATE(GETDATE())-1 AS date
, 43 AS "order"
, 'Pending Basic' AS measure
, COUNT(DISTINCT s.dim_account_key) AS value
FROM dwh.dim_service s
WHERE s.product_name = 'Basic'
AND s.account_id NOT IN (SELECT DISTINCT account_id FROM neon_reporting.test_accounts)
AND DATE(s.service_start_date) >= DATE(GETDATE())

UNION

SELECT DISTINCT DATE(GETDATE())-1 AS date
, 44 AS "order"
, 'Active 2Degrees' AS measure
, COUNT(DISTINCT s.account_id) AS value
FROM 
(
	SELECT DISTINCT account_id, dim_service_key, s.product_name, activation_method, s.service_status, s.service_start_date, s.service_end_date, s.coupon_code, s.evergent_order_id, s.evergent_ord_prod_id 
	FROM dwh.dim_service s
	WHERE s.coupon_code LIKE '2DGBB%'
	AND s.service_status IN ('ACTIVE', 'FINAL BILL') 
    AND s.activation_method = 'VOUCHER'
	AND DATE(GETDATE()) - INTERVAL '1 DAYS' BETWEEN DATE(s.service_start_date) AND DATE(s.service_end_date)
	AND s.account_id NOT IN (SELECT DISTINCT account_id FROM neon_reporting.test_accounts)
  	AND s.account_id NOT IN ('2110202318461679981', '2110200127461647532', '2110202313461679535', '2110202325461684235')
) s

UNION

SELECT DISTINCT DATE(GETDATE())-1 AS date
, 45 AS "ORDER"
, 'Direct Pending Churn - Remaining Month' AS measure 
, COUNT(DISTINCT account_id) AS value
FROM(
	SELECT DISTINCT account_id, dim_service_key, s.product_name, activation_method, s.service_status, s.service_start_date, s.service_end_date, s.coupon_code 
    FROM dwh.dim_service s
	INNER JOIN dwh.dim_date d ON DATE(d.date_dt) >= DATE(s.service_start_date) AND DATE(d.date_dt) <= DATE(s.service_end_date)
	WHERE  s.product_name IN ('Standard', 'Premium','Basic', 'Annual')
	AND s.service_status IN ('FINAL BILL')
	AND DATE(s.service_end_date) BETWEEN DATE(GETDATE()) AND last_day(GETDATE())
	AND s.activation_method NOT IN ('FREE TRIAL', 'VOUCHER')
	AND s.account_id NOT IN (SELECT DISTINCT account_id FROM neon_reporting.test_accounts)
	AND s.account_id NOT IN (SELECT DISTINCT account_id FROM vip)
	ORDER BY account_id, dim_service_key)
UNION

SELECT DISTINCT DATE(GETDATE() - interval '1 days') AS date
, 46 AS "ORDER"
, 'Paying Churn - Basic' AS measure --INCLUDES MONTHLY TO ANNUAL
, COUNT(DISTINCT account_id) AS value
FROM(
	SELECT DISTINCT account_id, dim_service_key, s.product_name, activation_method, s.service_status, s.service_start_date, s.service_end_date, s.coupon_code FROM dwh.dim_service s
	INNER JOIN dwh.dim_date d ON DATE(d.date_dt) >= DATE(s.service_start_date) AND DATE(d.date_dt) <= DATE(s.service_end_date)
	WHERE d.date_dt = DATE(GETDATE() - interval '1 days')
	AND s.product_name IN ('Basic')
	AND s.service_status IN ('FINAL BILL')
	AND DATE(s.service_end_date) = DATE(GETDATE() - interval '1 days')
	AND s.activation_method NOT IN ('FREE TRIAL', 'VOUCHER')
	AND s.account_id NOT IN (SELECT DISTINCT account_id FROM neon_reporting.test_accounts)
	AND s.account_id NOT IN (SELECT DISTINCT account_id FROM vip)
	ORDER BY account_id, dim_service_key
)

UNION

SELECT DISTINCT DATE(GETDATE() - interval '1 days') AS date
, 47 AS "ORDER"
, 'Paying Churn - Annual' AS measure --INCLUDES MONTHLY TO ANNUAL
, COUNT(DISTINCT account_id) AS value
FROM(
	SELECT DISTINCT account_id, dim_service_key, s.product_name, activation_method, s.service_status, s.service_start_date, s.service_end_date, s.coupon_code FROM dwh.dim_service s
	INNER JOIN dwh.dim_date d ON DATE(d.date_dt) >= DATE(s.service_start_date) AND DATE(d.date_dt) <= DATE(s.service_end_date)
	WHERE d.date_dt = DATE(GETDATE() - interval '1 days')
	AND s.product_name IN ('Annual')
	AND s.service_status IN ('FINAL BILL')
	AND DATE(s.service_end_date) = DATE(GETDATE() - interval '1 days')
	AND s.activation_method NOT IN ('FREE TRIAL', 'VOUCHER')
	AND s.account_id NOT IN (SELECT DISTINCT account_id FROM neon_reporting.test_accounts)
	AND s.account_id NOT IN (SELECT DISTINCT account_id FROM vip)
	ORDER BY account_id, dim_service_key
)
UNION

(
 
SELECT DISTINCT DATE(GETDATE() - interval '1 days') AS date
, 48 AS "ORDER"
, 'New Trialists L7D - Basic' AS measure
, COUNT(DISTINCT account_id) AS value
FROM
(
	SELECT DISTINCT s.account_id
  	FROM dwh.dim_service s	
  	INNER JOIN dwh.dim_date d ON DATE(d.date_dt) >= DATE(s.service_start_date) AND DATE(d.date_dt) <= DATE(s.service_end_date)
	WHERE d.date_dt = DATE(GETDATE())-1 
	AND s.product_name IN ('Basic')
	AND s.service_status NOT IN ('EXPIRED', 'INACTIVE')
	AND s.activation_method = 'FREE TRIAL'
	AND DATE(s.service_start_date) BETWEEN DATE(GETDATE())-7 AND DATE(GETDATE())-1 
	AND s.account_id NOT IN (SELECT DISTINCT account_id FROM neon_reporting.test_accounts)
	ORDER BY account_id
)
GROUP BY 1  
  
)

UNION

(
 
SELECT DISTINCT DATE(GETDATE() - interval '1 days') AS date
, 49 AS "ORDER"
, 'New Trialists L7D - Annual' AS measure
, COUNT(DISTINCT account_id) AS value
FROM
(
	SELECT DISTINCT s.account_id
  	FROM dwh.dim_service s	
  	INNER JOIN dwh.dim_date d ON DATE(d.date_dt) >= DATE(s.service_start_date) AND DATE(d.date_dt) <= DATE(s.service_end_date)
	WHERE d.date_dt = DATE(GETDATE())-1 
	AND s.product_name IN ('Annual')
	AND s.service_status NOT IN ('EXPIRED', 'INACTIVE')
	AND s.activation_method = 'FREE TRIAL'
	AND DATE(s.service_start_date) BETWEEN DATE(GETDATE())-7 AND DATE(GETDATE())-1 
	AND s.account_id NOT IN (SELECT DISTINCT account_id FROM neon_reporting.test_accounts)
	ORDER BY account_id
)
GROUP BY 1  
)
ORDER BY 2 ASC 
;