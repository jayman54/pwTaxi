-- Create the initial dbo.pickups_by_hour table

BEGIN TRY DROP TABLE  dbo.pickups_by_hour END TRY BEGIN CATCH END CATCH


	SELECT
			DATEADD(HOUR,A.pickup_hour,CAST(A.pickup_date AS DATETIME)) pickup_datetime
		,pickup_date
		,pickups
		INTO dbo.pickups_by_hour
	FROM
	(
		SELECT 
			CAST(pickup_datetime AS DATE) pickup_date
			,DATEPART(HOUR, pickup_datetime) pickup_hour
			,COUNT(*) pickups
		FROM nyctaxi_trip
		GROUP BY
		CAST(pickup_datetime AS DATE) 
		,DATEPART(HOUR, pickup_datetime)
	) A



-- Take a look a the data
SELECT TOP 10 * from dbo.pickups_by_hour

-- Check to see if every hour exists for every day
SELECT
	pickup_date
	,COUNT(*) hour_count
FROM
	dbo.pickups_by_hour
GROUP BY pickup_date
HAVING
	COUNT(*) <> 24
ORDER BY
	1

-- Build temp table to produce every hour for every day
-- 1:00

BEGIN TRY DROP TABLE  #DateTime END TRY BEGIN CATCH END CATCH

SELECT
	 DATEADD(HOUR,H.pickup_hour,CAST(D.pickup_date AS DATETIME)) pickup_datetime
	,D.pickup_date
	,H.pickup_hour
	INTO #DateTime
FROM
	(
	SELECT DISTINCT CAST(pickup_datetime AS DATE) pickup_date
	FROM nyctaxi_trip
	) D,
	(
	SELECT DISTINCT	DATEPART(HOUR, pickup_datetime) pickup_hour
	FROM nyctaxi_trip
	) H


-- Create the temp table pickups_by_hour table to include a row for every hour of every day

	BEGIN TRY DROP TABLE  #pickups_by_hour END TRY BEGIN CATCH END CATCH

	SELECT
		D.pickup_datetime
		,D.pickup_date
		,ISNULL(P.pickups,0) pickups

		-- Add windowing functions for later processing
		,LAG(ISNULL(P.pickups,0) ,1) OVER( ORDER BY D.pickup_datetime) pickups_hour_lag_1 
		,LEAD(ISNULL(P.pickups,0) ,1) OVER( ORDER BY D.pickup_datetime) pickups_hour_lead_1 
		INTO #pickups_by_hour
	FROM
		#DateTime D LEFT JOIN
	
		(
			-- Original query from above
			SELECT
					DATEADD(HOUR,A.pickup_hour,CAST(A.pickup_date AS DATETIME)) pickup_datetime
				,pickup_date
				,pickups
			FROM
			(
				SELECT 
					CAST(pickup_datetime AS DATE) pickup_date
					,DATEPART(HOUR, pickup_datetime) pickup_hour
					,COUNT(*) pickups
				FROM nyctaxi_trip
				GROUP BY
				CAST(pickup_datetime AS DATE) 
				,DATEPART(HOUR, pickup_datetime)
			) A
			-- Original query from above

		) P ON D.pickup_datetime = P.pickup_datetime	


-- Check to see if every hour exists for every day

SELECT
	pickup_date
	,COUNT(*) hour_count
FROM
	#pickups_by_hour
GROUP BY pickup_date
HAVING
	COUNT(*) <> 24
ORDER BY
	1

-- Check to see if any values fall below a certain threshold
-- Data Cleaning Step
SELECT TOP 10 * 
from #pickups_by_hour
where pickups = 0

-- Replace the missing value with the average of the value prior and post the missing value
DECLARE @hourLead INT = 5

BEGIN TRY DROP TABLE  dbo.pickups_by_hour END TRY BEGIN CATCH END CATCH


SELECT 
	pickup_datetime
	,pickup_date
	,DATEADD(HOUR, @hourLead, pickup_datetime) target_pickup_datetime
	,DATEPART(HOUR, DATEADD(HOUR, @hourLead, pickup_datetime)) target_pickup_hour
	,DATEPART(DW, DATEADD(HOUR, @hourLead, pickup_datetime)) target_pickup_dw
	,CASE
		WHEN (pickups=0 AND pickups_hour_lag_1 > 0 AND pickups_hour_lead_1 > 0)
			THEN (pickups_hour_lag_1 + pickups_hour_lead_1) / 2
			ELSE pickups
		END pickups
	INTO dbo.pickups_by_hour
FROM
	#pickups_by_hour

-- Check to see value
SELECT TOP 10 * 
from dbo.pickups_by_hour
where pickup_datetime = '2013-03-10 02:00:00.000'


-- Tag the data with our target variable
--BEGIN TRY DROP TABLE  dbo.pickups_train END TRY BEGIN CATCH END CATCH

--SELECT 
--	H.*
--	,P.pickups target_pickups 
--	INTO dbo.pickups_train 
--FROM 
--	dbo.pickups_by_hour H LEFT JOIN
--	dbo.pickups_by_hour P ON H.target_pickup_datetime = P.pickup_datetime


----Inspect the data
--SELECT TOP 100 *
--FROM
--	dbo.pickups_train
	