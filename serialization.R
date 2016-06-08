# A collection of functions useful to serialize/unserialize R objects
# into/from a database. These functions are modeled after well known
# R functions:
#
#   - dbReadRDS/dbSaveRDS: modeled after readRDS/saveRDS
#   - dbLoad/dbSave: modeled after load/save
#
# For these functions to work, powershell needs to be installed. It's
# installed on all Windows clients by default, might be missing only
# on Windows servers. Also, scripts should be allowed to execute:
#
#   Set-ExecutionPolicy Unrestricted
#
# The table, named "rdata" by default, used to store the R objects must
# have at least two columns:
#   - one character column, named "key" by default
#   - one varbinary column, named "value" by default
#
# The table name, as well as the names of the two columns can be changed
# using the additional parameters.
#
# To create the table one can use the following query:
#   create table [rdata] ([key] varchar(999) primary key not null, [value] varbinary(max))
#
# To remove a row from the table:
#   delete from [rdata] where [key] = 'mykey'
#
# The following functions are useful to run SQL from R:
#   - rxExecuteSQLDDL - to create a table for example, or to delete some rows
#   - rxSqlServerTableExists - to check whether a table exists
#   - rxSqlServerDropTable - to drop a table
#   - rxOpen/rxClose - these are needed when working with the above

# Removes "Driver" from the rx connection string - powershell frawns on it.
# Author; Microsoft
convertConnectionString <- function(conStr) {
    
    parts <- strsplit(strsplit(conStr, split=";")[[1]], split="=")
    keys <- tolower(sapply(parts, "[", 1))
    vals <- sapply(parts, "[", 2)
    ff <- grepl("driver", keys)
    res <- paste(paste(keys[!ff], vals[!ff], sep="="), collapse=";")
    res
    
}

# A lower level API shared by dbSaveRDS and dbSave.
#
# Builds and executes a powershell script to save a binary
# (given as a string file path), into the database.
dbSaveRaw <- function(connectionString, key, filePath, table="rdata", keyColumn="key", objectColumn="value") {
    
    queryLine <- paste("$query = 'insert into [", table, "]([", keyColumn, "],[", objectColumn, "]) values (@key,@value)'", sep="")
    
    psScript <- paste(
                    "[CmdletBinding()]",
                    "Param(",
                    "[Parameter()]",
                    "[string]$ConnectionString,",
                    "[string]$Key,",
                    "[string]$File)",
                    "[byte[]]$dd = [System.IO.File]::ReadAllBytes($File)",
                    "$con = new-object System.Data.SqlClient.SqlConnection($ConnectionString)",
                    "$res = $con.Open()",
                    queryLine,
                    "$cmd = new-object System.Data.SqlClient.SqlCommand($query, $con)",
                    "$res = $cmd.Parameters.Add('@key', [System.Data.SqlDbType]'VarChar')",
                    "$res = $cmd.Parameters.Add('@value', [System.Data.SqlDbType]'VarBinary')",
                    "$res = $cmd.Parameters['@key'].Value = $Key",
                    "$res = $cmd.Parameters['@value'].Value = $dd",
                    "$res = $cmd.ExecuteNonQuery()",
                    "$res = $con.Close()",
                    sep="\n")
    
    scriptFile <- tempfile(fileext=".ps1")
    on.exit(unlink(scriptFile))
    
    cat(psScript, file=scriptFile)
    
    psCmd <- paste(
                scriptFile,
                " -Key ",
                key,
                " -File ",
                filePath,
                " -ConnectionString ",
                "'", convertConnectionString(connectionString), "'",
                sep="")
    
    shell(psCmd, shell="powershell.exe")

}

# A lower level API shared by dbReadRDS and dbLoad.
#
# Builds and executes a powershell script to read a binary
# from the database and store it into a local file.
dbReadRaw <- function(connectionString, key, filePath, table="rdata", keyColumn="key", objectColumn="value") {
    
    queryLine <- paste("$query = 'select [", objectColumn, "] from [", table, "] where [", keyColumn, "] = @key'", sep="")
    
    psScript <- paste(
                    "[CmdletBinding()]",
                    "Param(",
                    "[Parameter()]",
                    "[string]$ConnectionString,",
                    "[string]$Key,",
                    "[string]$File)",
                    "$con = new-object System.Data.SqlClient.SqlConnection($ConnectionString)",
                    "$res = $con.Open()",
                    queryLine,
                    "$cmd = new-object System.Data.SqlClient.SqlCommand($query, $con)",
                    "$res = $cmd.Parameters.Add('@key', [System.Data.SqlDbType]'VarChar')",
                    "$res = $cmd.Parameters['@key'].Value = $Key",
                    "[byte[]]$dd = $cmd.ExecuteScalar()",
                    "$res = $con.Close()",
                    "$res = [IO.File]::WriteAllBytes($File, $dd)",
                    sep="\n")
    
    scriptFile <- tempfile(fileext=".ps1")
    on.exit(unlink(scriptFile))
    
    cat(psScript, file=scriptFile)
    
    psCmd <- paste(
                scriptFile,
                " -Key ",
                key,
                " -File ",
                filePath,
                " -ConnectionString ",
                "'", convertConnectionString(connectionString), "'",
                sep="")
    
    tt <- try(shell(psCmd, shell="powershell.exe"), silent=T)
    
}

# The dbSaveRDS and dbReadRDS functions are the database equivalents of R's
# readRDS/saveRDS APIs.

# The database equivalent of saveRDS. Serializes an R object and saves it into
# a varbinary column in the database. The row to insert the key is identified by
# a character string (key).
# The table name, the name of the key column and the name of the object column can
# be controlled by the table, keyColumn and objectColumn parameters.
dbSaveRDS <- function(connectionString, key, object, table="rdata", keyColumn="key", objectColumn="value", compress=F) {
    
    rawCon <- tempfile()
    on.exit(unlink(rawCon))
    saveRDS(object = object, file = rawCon) #, compress=FALSE)

    dbSaveRaw(connectionString, key, filePath=rawCon, table=table, keyColumn=keyColumn, objectColumn=objectColumn)    

}

dbReadRDS <- function(connectionString, key, table="rdata", keyColumn="key", objectColumn="value") {

    rawCon <- tempfile()
    on.exit(unlink(rawCon))

    tt <- try(dbReadRaw(connectionString, key, rawCon, table=table, keyColumn=keyColumn, objectColumn=objectColumn), silent=T)
    res <- if(inherits(tt, "try-error")) NULL else readRDS(rawCon)
    
    return(res)

}

# The dbSave and dbLoad functions are the database equivalents of R's
# save/load APIs.

# The database equivalent of "save". Serializes a group of R objects and saves them
# into a varbinary column in the database. The row to insert the key is identified by
# a character string (key).
#
# The table name, the name of the key column and the name of the object column can
# be controlled by the table, keyColumn and objectColumn parameters.
dbSave <- function(connectionString, key, ..., table="rdata", keyColumn="key", objectColumn="value", compress=F) {
    
    objectNames <- as.character(substitute(list(...)))[-1]
    rawCon <- tempfile()
    on.exit(unlink(rawCon))
    
    save(list=objectNames, file=rawCon, compress=compress)

    dbSaveRaw(connectionString, key, filePath=rawCon, table=table, keyColumn=keyColumn, objectColumn=objectColumn)
              
}

dbLoad <- function(connectionString, key, table="rdata", envir=parent.frame(), keyColumn="key", objectColumn="value") {
    
    rawCon <- tempfile()
    on.exit(unlink(rawCon))
    
    tt <- try(dbReadRaw(connectionString, key, rawCon, table=table, keyColumn=keyColumn, objectColumn=objectColumn), silent=T)
    if(!inherits(tt, "try-error")) {
        load(file=rawCon, envir=envir)
    }
    
}
