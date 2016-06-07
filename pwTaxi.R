
#set up variables
sqlConnString <- "Driver=SQL Server;Server=JrsSql2016e;Database=taxi;Uid=RServicesUser;Pwd=RServicesUser"
trainTable <- "pickups_train"
#trainTable <- "nyctaxi_sample"

sqlRowsPerRead = 5000

#Create the data set
sqlTaxiDS <- RxSqlServerData(connectionString = sqlConnString,
                             table = trainTable, rowsPerRead = sqlRowsPerRead)

#Get some basic information on the table
rxGetVarInfo(data = sqlTaxiDS)

#Need a local cache directory
sqlShareDir <- paste("c:\\AllShare\\", Sys.getenv("USERNAME"), sep="")
dir.create(sqlShareDir, recursive = TRUE)

#Set the compute context
sqlWait <- TRUE
sqlConsoleOutput <- FALSE

sqlCompute <- RxInSqlServer(connectionString = sqlConnString, shareDir = sqlShareDir,
                            wait = sqlWait,  consoleOutput = sqlConsoleOutput)

# Set the compute context to compute in SQL Server
rxSetComputeContext(sqlCompute)
#rxSetComputeContext(computeContext = 'local')

#Display summary statistics
rxSummary(formula = ~target_pickup_hour + target_pickup_dw, data = sqlTaxiDS)
#rxSummary(formula = ~fare_amount + tip_amount, data = sqlTaxiDS)

rxHistogram(~log(fare_amount), data = sqlTaxiDS,numBreaks = 30)

