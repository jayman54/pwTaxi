DECLARE @y INT = (SELECT AVG(target_pickups) FROM [taxi].[db_datareader].[pickups_score_train])

SELECT 
	1 -
	(
	SUM(SQUARE((forecasted_pickups - target_pickups)))/
	SUM(SQUARE((@y - target_pickups)))
	) train_R_Squared_Coefficient_of_determination
FROM [taxi].[db_datareader].[pickups_score_train]

SET @y =  (SELECT AVG(target_pickups) FROM [taxi].[db_datareader].[pickups_score_test])

SELECT 
	1 -
	(
	SUM(SQUARE((forecasted_pickups - target_pickups)))/
	SUM(SQUARE((@y - target_pickups)))
	) test_R_Squared_Coefficient_of_determination
FROM [taxi].[db_datareader].[pickups_score_test]