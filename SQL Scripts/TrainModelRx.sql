use [taxi]
go

set ansi_nulls on
go

set quoted_identifier on
go

DROP PROCEDURE IF EXISTS TrainModelRx
GO

/* 
 Description: This file creates the procedure to train a Microsoft R model for the customer churn template.
*/
create procedure [TrainModelRx]
as
begin
  declare @inquery nvarchar(max) = N'
	SELECT  [id]
      ,[pickup_datetime]
      ,[target_pickup_hour]
      ,[target_pickup_dw]
      ,[pickups_hour_lag_0]
      ,[pickups_hour_lag_1]
      ,[pickups_hour_lag_2]
      ,[pickups_hour_lag_3]
      ,[pickups_hour_lag_4]
      ,[pickups_hour_lag_5]
      ,[pickups_hour_lag_6]
      ,[pickups_hour_lag_7]
      ,[pickups_date_lag_1]
      ,[pickups_date_lag_2]
      ,[pickups_date_lag_3]
      ,[pickups_date_lag_4]
      ,[pickups_date_lag_5]
      ,[pickups_date_lag_6]
      ,[pickups_date_lag_7]
      ,[target_pickups]
  FROM [taxi].[dbo].[v_pickups_test]'

  -- Insert the trained model into a database table
  insert into dbo.[models]
  exec sp_execute_external_script @language = N'R',
                                  @script = N'
ccColInfo <- list(
  target_pickup_hour = list(
    type = "factor",
    levels = as.character(0:23)),
  target_pickup_dw = list(
    type = "factor",
    levels = as.character(1:7),
    newLevels = c("Sunday","Monday","Tuesday","Wednesday","Thursday","Friday","Saturday"))
)

InputDataSet <- rxImport(InputDataSet, colInfo = ccColInfo)

#build model
F <- "target_pickups ~ target_pickup_hour +	target_pickup_dw + pickups_hour_lag_0 + pickups_hour_lag_1 +
      pickups_hour_lag_2 + pickups_hour_lag_3 +	pickups_hour_lag_4 + pickups_hour_lag_5 +
      pickups_hour_lag_6 + pickups_hour_lag_7 + pickups_date_lag_1 + pickups_date_lag_2 +	
      pickups_date_lag_3 + pickups_date_lag_4 + pickups_date_lag_5 + pickups_date_lag_6 +	
      pickups_date_lag_7"


bd <- rxBTrees(formula = F, data=InputDataSet,
               maxDepth = 6, minSplit = 2, nTree = 50,
               lossFunction = "gaussian", learningRate = 0.1)

## Serialize model and put it in data frame
trained_model <- data.frame(model=as.raw(serialize(bd, connection=NULL)));'
,@input_data_1 = @inquery
,@output_data_1_name = N'trained_model';
end
go

execute TrainModelRx
go

