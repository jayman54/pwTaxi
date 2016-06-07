--Compare table to desired format

--SELECT TOP 100 *
--FROM
--	dbo.pickups_by_hour

--SELECT TOP 100 *
--FROM
--	dbo.pickups_by_hour_lag
--ORDER BY 1


DECLARE @hourLead INT = 5

BEGIN TRY DROP TABLE  dbo.pickups_by_hour_lag END TRY BEGIN CATCH END CATCH

SELECT
	 AP.pickup_datetime
	,AP.pickup_date
	,DATEADD(HOUR, @hourLead, AP.pickup_datetime) target_pickup_datetime
	,DATEPART(HOUR, DATEADD(HOUR, @hourLead, AP.pickup_datetime)) target_pickup_hour
	,DATEPART(DW, DATEADD(HOUR, @hourLead, AP.pickup_datetime)) target_pickup_dw
	,AP.pickups pickups_hour_lag_0
	,LAG(AP.pickups ,1) OVER( ORDER BY AP.pickup_datetime) pickups_hour_lag_1 
	,LAG(AP.pickups ,2) OVER( ORDER BY AP.pickup_datetime) pickups_hour_lag_2
	,LAG(AP.pickups ,3) OVER( ORDER BY AP.pickup_datetime) pickups_hour_lag_3
	,LAG(AP.pickups ,4) OVER( ORDER BY AP.pickup_datetime) pickups_hour_lag_4
	,LAG(AP.pickups ,5) OVER( ORDER BY AP.pickup_datetime) pickups_hour_lag_5
	,LAG(AP.pickups ,6) OVER( ORDER BY AP.pickup_datetime) pickups_hour_lag_6
	,LAG(AP.pickups ,7) OVER( ORDER BY AP.pickup_datetime) pickups_hour_lag_7
	,pickups_date_lag_1
	,pickups_date_lag_2
	,pickups_date_lag_3
	,pickups_date_lag_4
	,pickups_date_lag_5
	,pickups_date_lag_6
	,pickups_date_lag_7
	INTO dbo.pickups_by_hour_lag
FROM
	dbo.pickups_by_hour AP INNER JOIN
	(
		SELECT
			 pickup_date
			,LAG(pickups ,1) OVER( ORDER BY pickup_date) pickups_date_lag_1	
			,LAG(pickups ,2) OVER( ORDER BY pickup_date) pickups_date_lag_2	
			,LAG(pickups ,3) OVER( ORDER BY pickup_date) pickups_date_lag_3
			,LAG(pickups ,4) OVER( ORDER BY pickup_date) pickups_date_lag_4
			,LAG(pickups ,5) OVER( ORDER BY pickup_date) pickups_date_lag_5
			,LAG(pickups ,6) OVER( ORDER BY pickup_date) pickups_date_lag_6
			,LAG(pickups ,7) OVER( ORDER BY pickup_date) pickups_date_lag_7
		FROM
		(
		SELECT 
			pickup_date, 
			SUM(pickups) pickups   
		FROM dbo.pickups_by_hour 
		GROUP BY pickup_date
		) A
	) B ON AP.pickup_date = B.pickup_date


--Tag the data

DECLARE @hourLead INT = 5

BEGIN TRY DROP TABLE  dbo.pickups_train END TRY BEGIN CATCH END CATCH

SELECT 
		L.*
		,P.pickups target_pickups 
		INTO dbo.pickups_train
	FROM 
		dbo.pickups_by_hour_lag L LEFT JOIN
		dbo.pickups_by_hour P ON L.target_pickup_datetime = P.pickup_datetime
	WHERE 
		L.pickups_date_lag_7 IS NOT NULL


SELECT
	TOP 100 *
FROM 
	dbo.pickups_train