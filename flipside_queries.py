# Queries used to extract the desired on-chain information from Flipside app
# There are variables in some scripts below like the start date of the query and the period for aggregating (day,week,month)

#Metamask Swaps Over Time
metamask_swap = """
WITH new_users AS (
  SELECT
    cohort_date as date,
    COUNT(DISTINCT origin_from_address) as new_address
  FROM
    (SELECT
      origin_from_address,
      MIN(DATE(DATE_TRUNC('{0}',block_timestamp))) as cohort_date
    FROM
      crosschain.defi.ez_swaps
    WHERE blockchain IN ('Ethereum', 'Polygon', 'BSC')
      AND amount_in_usd is not NULL
      AND DATE_TRUNC('{0}',block_timestamp)>= '{1}'
      AND
      (CASE
        WHEN blockchain = 'BSC' THEN amount_in_usd < 10e6 AND amount_out_usd < 10e6
        ELSE amount_in_usd > 0
      END) = TRUE
      AND
      (CASE
          WHEN blockchain = 'Ethereum' THEN origin_to_address= '0x881d40237659c251811cec9c364ef91dc08d300c' -- Ethereum 
          WHEN blockchain = 'Polygon' THEN origin_to_address = lower('0x1a1ec25DC08e98e5E93F1104B5e5cdD298707d31') -- Polygon
          WHEN blockchain = 'BSC' THEN origin_to_address = lower('0x1a1ec25DC08e98e5E93F1104B5e5cdD298707d31') -- Polygon
        END) = True
    GROUP BY 1) AS tab_x
  GROUP BY date
  ORDER BY date ASC
),

active_users AS (
  SELECT
    DATE(DATE_TRUNC('{0}',block_timestamp)) AS date,
    COUNT(DISTINCT tx_hash) as swap_count,
    COUNT(DISTINCT  origin_from_address) as active_address,
    SUM(amount_in_usd) as vol_usd 
  FROM
    crosschain.defi.ez_swaps
  WHERE blockchain IN ('Ethereum', 'Polygon', 'BSC')
      AND amount_in_usd is not NULL
      AND DATE_TRUNC('{0}',block_timestamp)>= '{1}'
      AND
      (CASE
        WHEN blockchain = 'BSC' THEN amount_in_usd < 10e6 AND amount_out_usd < 10e6
        ELSE amount_in_usd > 0
      END) = TRUE
      AND
      (CASE
          WHEN blockchain = 'Ethereum' THEN origin_to_address= '0x881d40237659c251811cec9c364ef91dc08d300c' -- Ethereum 
          WHEN blockchain = 'Polygon' THEN origin_to_address = lower('0x1a1ec25DC08e98e5E93F1104B5e5cdD298707d31') -- Polygon
          WHEN blockchain = 'BSC' THEN origin_to_address = lower('0x1a1ec25DC08e98e5E93F1104B5e5cdD298707d31') -- Polygon
        END) = True
  GROUP BY date
  ORDER BY date ASC)

SELECT
  a.date,
  active_address,
  new_address,
  active_address - new_address as existing_address,
  (new_address/active_address) as new_address_percent,
  swap_count,
  vol_usd
    
FROM
  active_users a
  LEFT JOIN  new_users n  ON a.date = n.date
ORDER BY date ASC

"""

#Metamask Swaps by Liquidity Sources Over Time
metamask_sources ="""
SELECT
  DATE(DATE_TRUNC('{0}',block_timestamp)) AS date,
  platform,
  COUNT(DISTINCT tx_hash) as swap_count,
  COUNT(DISTINCT  origin_from_address) as active_address,
  SUM(amount_in_usd) as vol_usd 
FROM
  crosschain.defi.ez_swaps
  WHERE blockchain IN ('Ethereum', 'Polygon', 'BSC')
      AND amount_in_usd is not NULL
      AND DATE_TRUNC('day',block_timestamp)>= '{1}'
      AND
      (CASE
        WHEN blockchain = 'BSC' THEN amount_in_usd < 10e6 AND amount_out_usd < 10e6
        ELSE amount_in_usd > 0
      END) = TRUE
      AND
      (CASE
          WHEN blockchain = 'Ethereum' THEN origin_to_address= '0x881d40237659c251811cec9c364ef91dc08d300c' -- Ethereum 
          WHEN blockchain = 'Polygon' THEN origin_to_address = lower('0x1a1ec25DC08e98e5E93F1104B5e5cdD298707d31') -- Polygon
          WHEN blockchain = 'BSC' THEN origin_to_address = lower('0x1a1ec25DC08e98e5E93F1104B5e5cdD298707d31') -- Polygon
        END) = True
    
GROUP BY date, platform
ORDER BY date ASC

"""

#Metamaks Top Users by SWap
top_users_swap = """
SELECT
  ROW_NUMBER() OVER(ORDER BY swap_count DESC) AS rank,
  *
FROM
(SELECT
  origin_from_address,
  COUNT(DISTINCT tx_hash) as swap_count
  --SUM(amount_in_usd) as vol_usd 
FROM
  crosschain.defi.ez_swaps
  WHERE blockchain IN ('Ethereum', 'Polygon', 'BSC')
      AND amount_in_usd is not NULL
      AND
      (CASE
        WHEN blockchain = 'BSC' THEN amount_in_usd < 10e6 AND amount_out_usd < 10e6
        ELSE amount_in_usd > 0
      END) = TRUE
      AND
      (CASE
          WHEN blockchain = 'Ethereum' THEN origin_to_address= '0x881d40237659c251811cec9c364ef91dc08d300c' -- Ethereum 
          WHEN blockchain = 'Polygon' THEN origin_to_address = lower('0x1a1ec25DC08e98e5E93F1104B5e5cdD298707d31') -- Polygon
          WHEN blockchain = 'BSC' THEN origin_to_address = lower('0x1a1ec25DC08e98e5E93F1104B5e5cdD298707d31') -- Polygon
        END) = True
    
GROUP BY origin_from_address
ORDER BY swap_count DESC
LIMIT 50) 
"""

#MEtamask Top Users by USD Volume
top_users_vol = """
SELECT
  ROW_NUMBER() OVER(ORDER BY vol_usd DESC) AS rank,
  *
FROM
(SELECT
  origin_from_address,
  SUM(amount_in_usd) as vol_usd 
FROM
  crosschain.defi.ez_swaps
  WHERE blockchain IN ('Ethereum', 'Polygon', 'BSC')
      AND amount_in_usd is not NULL
      AND
      (CASE
        WHEN blockchain = 'BSC' THEN amount_in_usd < 10e6 AND amount_out_usd < 10e6
        ELSE amount_in_usd > 0
      END) = TRUE
      AND
      (CASE
          WHEN blockchain = 'Ethereum' THEN origin_to_address= '0x881d40237659c251811cec9c364ef91dc08d300c' -- Ethereum 
          WHEN blockchain = 'Polygon' THEN origin_to_address = lower('0x1a1ec25DC08e98e5E93F1104B5e5cdD298707d31') -- Polygon
          WHEN blockchain = 'BSC' THEN origin_to_address = lower('0x1a1ec25DC08e98e5E93F1104B5e5cdD298707d31') -- Polygon
        END) = True
    
GROUP BY origin_from_address
ORDER BY vol_usd DESC
LIMIT 50) 


"""

#Stickiness Ratio
stickiness_ratio = """"
WITH DAU_tab AS (
  SELECT
    DATE(DATE_TRUNC('{0}', date)) AS month,
    AVG(active_address) as dau
  FROM (
    SELECT
      DATE(block_timestamp) AS date,
      COUNT(DISTINCT  origin_from_address) as active_address
    FROM
      crosschain.defi.ez_swaps
    WHERE blockchain IN ('Ethereum', 'Polygon', 'BSC')
        AND amount_in_usd is not NULL
        AND DATE_TRUNC('{1}',block_timestamp)>= '2022-01-01'
        AND
        (CASE
          WHEN blockchain = 'BSC' THEN amount_in_usd < 10e6 AND amount_out_usd < 10e6
          ELSE amount_in_usd > 0
        END) = TRUE
        AND
        (CASE
            WHEN blockchain = 'Ethereum' THEN origin_to_address= '0x881d40237659c251811cec9c364ef91dc08d300c' -- Ethereum 
            WHEN blockchain = 'Polygon' THEN origin_to_address = lower('0x1a1ec25DC08e98e5E93F1104B5e5cdD298707d31') -- Polygon
            WHEN blockchain = 'BSC' THEN origin_to_address = lower('0x1a1ec25DC08e98e5E93F1104B5e5cdD298707d31') -- Polygon
          END) = True
    GROUP BY date
    ORDER BY date ASC)
  GROUP BY month

),

MAU_tab AS (
  SELECT
      DATE(DATE_TRUNC('{0}', block_timestamp)) AS month,
      COUNT(DISTINCT  origin_from_address) as mau
    FROM
      crosschain.defi.ez_swaps
    WHERE blockchain IN ('Ethereum', 'Polygon', 'BSC')
        AND amount_in_usd is not NULL
        AND DATE_TRUNC('month',block_timestamp)>= '{1}'
        AND
        (CASE
          WHEN blockchain = 'BSC' THEN amount_in_usd < 10e6 AND amount_out_usd < 10e6
          ELSE amount_in_usd > 0
        END) = TRUE
        AND
        (CASE
            WHEN blockchain = 'Ethereum' THEN origin_to_address= '0x881d40237659c251811cec9c364ef91dc08d300c' -- Ethereum 
            WHEN blockchain = 'Polygon' THEN origin_to_address = lower('0x1a1ec25DC08e98e5E93F1104B5e5cdD298707d31') -- Polygon
            WHEN blockchain = 'BSC' THEN origin_to_address = lower('0x1a1ec25DC08e98e5E93F1104B5e5cdD298707d31') -- Polygon
          END) = True
    GROUP BY month
    ORDER BY month ASC
)

SELECT
  mau.month,
  ROUND(dau/mau, 3) AS stickniness_ratio
    
FROM
  MAU_tab mau
  LEFT JOIN  DAU_tab dau ON mau.month = dau.month
ORDER BY mau.month ASC

"""