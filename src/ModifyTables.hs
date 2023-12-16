{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE OverloadedStrings #-}
{-# OPTIONS_GHC -Wno-incomplete-patterns #-}

module ModifyTables (
    dropTable,
    createTable,
) where
import DataFrame (DataFrame (DataFrame), Column (Column), ColumnType(..))

-- TEMP IMPORTS
import ParsingParsec (parseSql, ParsedStatement(..))
import qualified Lib3
import Lib2 (getTable)
import InMemoryTables (database)
import Lib1 (renderDataFrameAsTable)


type ErrorMessage = String
type TableName = String

-- DROP TABLE
dropTable :: TableName -> Either ErrorMessage DataFrame
dropTable tableName = Right $ DataFrame [Column "Table deletetion not implemented." StringType] []

-- CREATE TABLE
-- Create Table with Column Info got from parseSql
createTable :: TableName -> [(String, ColumnType)] -> Either ErrorMessage (TableName, DataFrame)
createTable tableName columnInfo = do
    if isValidTableName tableName then
        createDataFrame columnInfo >>= \df -> Right (tableName, df)
    else
        Left "Unable to create Table"
    where 
        isValidTableName :: TableName -> Bool
        isValidTableName tableName = 
            if not $ null tableName && not alreadyExists then 
                True
            else
                False
        alreadyExists = False



createDataFrame :: [(String, ColumnType)] -> Either ErrorMessage DataFrame
createDataFrame columnInfo = Right $ DataFrame (createColumns columnInfo) []

createColumns :: [(String, ColumnType)] -> [Column]
createColumns [] = []
createColumns ((columnName, columnType) : xs) = 
    (Column columnName columnType) : createColumns xs
