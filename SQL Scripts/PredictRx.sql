use [taxi]
go

set ansi_nulls on
go

set quoted_identifier on
go

DROP PROCEDURE IF EXISTS PredictPickupsRx
GO

/* 
 Description: This file creates the procedure to predict churn outcome based on the Microsoft R model previously built.
*/
create procedure PredictPickupsRx @inquery nvarchar(max)
as
begin
  declare @modelt varbinary(max) = (select TOP 1 [model] from dbo.[models]);
  insert into [db_datareader].[pickups_score_test] 
  exec sp_execute_external_script @language = N'R',
                                  @script = N'

mod <- unserialize(as.raw(model));

#Set column information as we did in R
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

score <- rxPredict(modelObject = mod,
          data = InputDataSet,
          outData = NULL,
          predVarNames = "forecasted_pickups",
          writeModelVars = FALSE,
          extraVarsToWrite = c("target_pickups","id"),
          overwrite = TRUE)


OutputDataSet <- data.frame(score)'
,@input_data_1 = @inquery
,@output_data_1_name = N'OutputDataSet'
,@params = N'@model varbinary(max)'
,@model = @modelt;
end
go

declare @query_string nvarchar(max)
set @query_string='
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
  FROM [taxi].[dbo].[v_pickups_test] 
'
execute PredictPickupsRx @inquery = @query_string;
go


