
#set up variables
sqlConnString <- "Driver=SQL Server;Server=JrsSql2016e;Database=taxi;Uid=RServicesUser;Pwd=RServicesUser"
trainTable <- "t_pickups_train"
testTable <- "v_pickups_test"

#Column information
ccColInfo <- list(
  target_pickup_hour = list(
    type = "factor",
    levels = as.character(0:23)),
  target_pickup_dw = list(
    type = "factor",
    levels = as.character(1:7),
    newLevels = c("Sunday","Monday","Tuesday","Wednesday","Thursday","Friday","Saturday"))
)




#Create the data set
trainDS <- RxSqlServerData(connectionString = sqlConnString,
                             table = trainTable, rowsPerRead = 5000, colInfo = ccColInfo)

testDS <- RxSqlServerData(connectionString = sqlConnString,
                           table = testTable, rowsPerRead = 5000, colInfo = ccColInfo)

#Get some basic information on the table
rxGetVarInfo(data = trainDS)
rxGetVarInfo(data = testDS)

#Need a local cache directory
sqlShareDir <- paste("c:\\AllShare\\", Sys.getenv("USERNAME"), sep="")
dir.create(sqlShareDir, recursive = TRUE)

#Set the compute context
sqlCompute <- RxInSqlServer(connectionString = sqlConnString, shareDir = sqlShareDir,
                            wait = TRUE,  consoleOutput = TRUE)

# Set the compute context to compute in SQL Server
rxSetComputeContext(sqlCompute)
#rxSetComputeContext(computeContext = 'local')

#Display summary statistics
rxSummary(formula = ~target_pickup_hour + target_pickup_dw + target_pickups, data = trainDS)


#Perform some visualizations
rxHistogram(~target_pickups, data = trainDS)

rxHistogram(~target_pickups|target_pickup_dw, data = trainDS)


#build model
F <- "target_pickups ~ target_pickup_hour +	target_pickup_dw + pickups_hour_lag_0 + pickups_hour_lag_1 +
      pickups_hour_lag_2 + pickups_hour_lag_3 +	pickups_hour_lag_4 + pickups_hour_lag_5 +
      pickups_hour_lag_6 + pickups_hour_lag_7 + pickups_date_lag_1 + pickups_date_lag_2 +	
      pickups_date_lag_3 + pickups_date_lag_4 + pickups_date_lag_5 + pickups_date_lag_6 +	
      pickups_date_lag_7"


bd <- rxBTrees(formula = F, data=trainDS,
               maxDepth = 6, minSplit = 2, nTree = 50,
               lossFunction = "gaussian", learningRate = 0.1)

summary(bd)
plot(bd)


#Score the model
#Training data
scoreTrainTable <- "pickups_score_train"

scoreTrainDS <- RxSqlServerData(table = scoreTrainTable,
                                  connectionString = sqlConnString, rowsPerRead = 5000 )

#Drop the destination table if exists
if (rxSqlServerTableExists(scoreTrainTable))
  rxSqlServerDropTable(scoreTrainTable)

#Score the trainin data
rxPredict(modelObject = bd,
          data = trainDS,
          outData = scoreTrainDS,
          predVarNames = "forecasted_pickups",
          writeModelVars = FALSE,
          extraVarsToWrite = c("id","target_pickups"),
          overwrite = TRUE)


#Testing data
scoreTestTable <- "pickups_score_test"

scoreTestDS <- RxSqlServerData(table = scoreTestTable,
                                  connectionString = sqlConnString, rowsPerRead = 5000 )

#Drop the destination table if exists
if (rxSqlServerTableExists(scoreTestTable))
  rxSqlServerDropTable(scoreTestTable)

#Score the trainin data
rxPredict(modelObject = bd,
          data = testDS,
          outData = scoreTestDS,
          predVarNames = "forecasted_pickups",
          writeModelVars = FALSE,
          extraVarsToWrite = c("id","target_pickups"),
          overwrite = TRUE)

#Serialize the mode and save it in SQL
bd.df <- data.frame(model=as.raw(serialize(bd, connection=NULL)))
