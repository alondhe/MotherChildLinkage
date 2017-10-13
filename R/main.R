
#' Get family IDs from native data set
#'
#' @description
#' \code{getFamilyIds} obtains the family IDs from the native data source and writes to ff object.
#'
#' @details
#' \code{getFamilyIds} obtains the family IDs from the native data source, which is useful if family IDs are time-varying and cannot
#'                     be stored in the payer_plan_period.family_source_value field.
#' 
#' @param nativeConnectionDetails          An R object of type ConnectionDetails (details for the function that contains server info, database type, optionally username/password, port)
#' @param nativeDatabaseSchema             Fully qualified name of database schema that contains the native data set.
#' @param nativeTable                      The name of the table that holds the family Id in the native data set.
#' @param nativePersonId                   The name of the person_id field in the native data set.
#' @param nativeFamilyId                   The name of the family_id field in the native data set.
#' @param nativeStartDate                  The name of the start date field for the observation period / payer plan period
#' @param nativeEndDate                    The name of the end date field for the observation period / payer plan period
#' @return                                 ffdf object with the person - family Id linkage
#' 
#' @export
getFamilyIds <- function(nativeConnectionDetails, 
                         nativeDatabaseSchema, 
                         nativeTable, 
                         nativePersonId,
                         nativeFamilyId, 
                         nativeStartDate, 
                         nativeEndDate)
{
  sql <- SqlRender::renderSql(sql = "select distinct @nativePersonId as person_id, 
                                    @nativeFamilyId as family_source_value, 
                                    @nativeStartDate as payer_plan_period_start_date, 
                                    @nativeEndDate as payer_plan_period_end_date
                                    from @nativeDatabaseSchema.@nativeTable;",
                              nativeDatabaseSchema = nativeDatabaseSchema,
                              nativeTable = nativeTable,
                              nativePersonId = nativePersonId,
                              nativeFamilyId = nativeFamilyId,
                              nativeStartDate = nativeStartDate,
                              nativeEndDate = nativeEndDate)$sql
  sql <- SqlRender::translateSql(sql = sql, targetDialect = nativeConnectionDetails$dbms)$sql
  
  connection <- DatabaseConnector::connect(connectionDetails = nativeConnectionDetails)
  ffdf <- DatabaseConnector::querySql.ffdf(connection = connection, sql = sql)
  DatabaseConnector::disconnect(connection)
  return (ffdf)
}


#' Generate Mother-Child Linkages
#'
#' @description
#' \code{generate} creates descriptive statistics summary for an entire OMOP CDM instance.
#'
#' @details
#' \code{generate} creates descriptive statistics summary for an entire OMOP CDM instance.
#' 
#' @param connectionDetails                An R object of type ConnectionDetails (details for the function that contains server info, 
#'                                         database type, optionally username/password, port)
#' @param cdmDatabaseSchema    	           Fully qualified name of database schema that contains OMOP CDM (including Vocabulary). 
#'                                         On SQL Server, this should specifiy both the database and the schema, so for example, on SQL Server, 'cdm_instance.dbo'.
#' @param resultsDatabaseSchema		         Fully qualified name of database schema that holds the pregnancy_episodes table.
#'                                         On SQL Server, this should specifiy both the database and the schema, so for example, on SQL Server, 'cdm_results.dbo'.
#' @param pppDatabaseSchema                Fully qualified name of database schema that holds the payer_plan_period table with family_source_value populated. 
#'                                         Default is cdmDatabaseSchema. On SQL Server, this should specifiy both the database and the schema, 
#'                                         so for example, on SQL Server, 'cdm_scratch.dbo'.                                          
#' @param motherRelationshipId             (OPTIONAL) The concept Id to relate a mother to a child. By default, it is 40478925.
#' @param childRelationshipId              (OPTIONAL) The concept Id to relate a child to a parent. By default, it is 40485452.
#' 
#' @export
generate <- function(connectionDetails,
                     cdmDatabaseSchema,
                     resultsDatabaseSchema,
                     pppDatabaseSchema = cdmDatabaseSchema,
                     motherRelationshipId = 40478925,
                     childRelationshipId = 40485452,
                     ffdf = NULL)
{
  checkPregnancyEpisodes <- function()
  {
    connection <- DatabaseConnector::connect(connectionDetails = connectionDetails)
    
    sql <- "select count(*) from INFORMATION_SCHEMA.TABLES
            where table_schema = '@resultsDatabaseSchema' and table_name = 'pregnancy_episodes';"
    sql <- SqlRender::renderSql(sql = sql, resultsDatabaseSchema = resultsDatabaseSchema)$sql
    sql <- SqlRender::translateSql(sql = sql, targetDialect = connectionDetails$dbms)$sql
    exists <- DatabaseConnector::querySql(connection = connection, sql = sql)

    if (exists == 0)
    {
      return (FALSE)
    }
    
    sql <- "select count(*) from @resultsDatabaseSchema.pregnancy_episodes;"
    sql <- SqlRender::renderSql(sql = sql, resultsDatabaseSchema = resultsDatabaseSchema)$sql
    sql <- SqlRender::translateSql(sql = sql, targetDialect = connectionDetails$dbms)$sql
    
    rowCount <- DatabaseConnector::querySql(connection = connection, sql = sql)
    DatabaseConnector::disconnect(connection = connection)
    return (rowCount > 0)
  }
  
  # check if pregnancy episodes exist -------------------------------------------------
  
  if (!checkPregnancyEpisodes())
  {
    stop(paste("Pregnancy Episodes not found in:", resultsDatabaseSchema, sep = " "))
  }
  
  # drop existing linkages------------------------------------------------------------
  
  clearSql <- "delete from @cdmDatabaseSchema.fact_relationship
               where relationship_concept_id in (@motherRelationshipId, @childRelationshipId);"
  clearSql <- SqlRender::renderSql(sql = clearSql, 
                                   cdmDatabaseSchema = cdmDatabaseSchema,
                                   motherRelationshipId = motherRelationshipId, 
                                   childRelationshipId = childRelationshipId)$sql
  clearSql <- SqlRender::translateSql(sql = clearSql, 
                                      targetDialect = connectionDetails$dbms)$sql
  
  # insert family Id linkage if needed -----------------------------------------------
  
  if (!is.null(ffdf))
  {
    DatabaseConnector::insertTable(connection = connection, 
                                   tableName = paste(pppDatabaseSchema, "payer_plan_period", sep = "."), 
                                   data = ffdf, 
                                   dropTableIfExists = TRUE, 
                                   createTable = TRUE, 
                                   tempTable = FALSE)
  }
  
  # generate new linkages------------------------------------------------------------
  
  generateSql <- SqlRender::loadRenderTranslateSql(sqlFilename = "ConstructCohorts.sql", 
                                           packageName = "MotherChildLinkage", 
                                           dbms = connectionDetails$dbms,
                                           cdmDatabaseSchema = cdmDatabaseSchema,
                                           resultsDatabaseSchema = resultsDatabaseSchema,
                                           pppDatabaseSchema = pppDatabaseSchema,
                                           motherRelationshipId = motherRelationshipId,
                                           childRelationshipId = childRelationshipId)

  connection <- DatabaseConnector::connect(connectionDetails = connectionDetails)
  DatabaseConnector::executeSql(connection = connection, sql = clearSql)
  DatabaseConnector::executeSql(connection = connection, sql = generateSql)
  DatabaseConnector::disconnect(connection = connection)
}