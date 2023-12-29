{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE OverloadedStrings #-}
{-# OPTIONS_GHC -Wno-incomplete-patterns #-}
{-# OPTIONS_GHC -Wno-unrecognised-pragmas #-}
{-# HLINT ignore "Use camelCase" #-}

module Main where

import Data.Yaml hiding (Value)
import Control.Concurrent
import Control.Monad
import InMemoryTables (TableName)
import DataFrame
import Lib1 (renderDataFrameAsTable)
import Lib2 (mergeListOfDataFrames, mergeDataFrames, filterDataframe, columnListDF)
import Lib3 (deleteFunction, updateFunction, insertFunction, jsonToDataframe, dataframeToJson)
import Web.Scotty
import Data.Text.Lazy.Encoding (decodeUtf8)
import ParsingParsec
import Data.ByteString.Lazy.Char8 as LBS ( unpack, pack)
import qualified Data.ByteString.Char8 as BS
import Data.Text.Lazy as TL (pack)
import Control.Applicative ((<|>))
import Control.Monad.IO.Class (liftIO)


main :: IO ()
main = do
    inDBTables <- loadTableListFromDB
    _loadedTables <- loadTablesFromDB inDBTables
    case _loadedTables of 
        Left err -> do 
            putStrLn err
        Right loadedTables -> do
            mvar_tables <- newMVar loadedTables
            saveThreadID <- forkIO $ thread_saveTables mvar_tables
            scotty 8080 $ do

                post "/" $ do
                    body <- body
                    let idk = decodeEither' (BS.toStrict body) :: Either ParseException ParsedStatement
                    case idk of
                        Left err -> text $ "An error occured.\n" <> TL.pack (prettyPrintParseException err)
                        Right parsedStatement -> do
                            res <- liftAndCatchIO $ executeParsedStatement parsedStatement mvar_tables

                            case res of
                                Left err -> text $ TL.pack err
                                Right dataFrame -> text $ TL.pack (renderDataFrameAsTable 1000 dataFrame)
                                    

-- run server & run
-- curl -X POST -d "select * from a;" http://localhost:8080
-- for calling this post method

executeParsedStatement :: ParsedStatement -> MVar [(TableName, DataFrame)] -> IO (Either ErrorMessage DataFrame)
-- SELECT
executeParsedStatement (SelectStatement columns tableNames conditions order) mvar_tables = do
    tables <- getMultipleTables_mvar tableNames mvar_tables
    case tables of
        Left err -> return $ Left err
        Right tableList -> do
            let dFrames = map snd tableList
            let dFrame = mergeListOfDataFrames dFrames
            case filterDataframe dFrame conditions of
                Just filteredDataframe -> do
                    let selectedColumns = columnListDF columns filteredDataframe
                    case selectedColumns of
                        Right selectResult -> return $ Right selectResult
                        Left _ -> return $ Left "Some columns were incorrect"
                Nothing -> return $ Left "Incorrect condition"
-- DELETE
executeParsedStatement (DeleteStatement name conditions) mvar_tables = do
    table <- getTable_mvar name mvar_tables
    case table of
        Left err -> return $ Left err
        Right table -> do
            let dFrame = snd table
            let modifiedDataFrame = deleteFunction dFrame conditions
            let tableName = fst table
            res <- replaceTable_mvar (tableName, modifiedDataFrame) mvar_tables
            case res of
                Left err -> return $ Left err
                Right _ -> return $ Left "DELETE"
-- UPDATE
executeParsedStatement (UpdateStatement name updates conditions) mvar_tables = do
    table <- getTable_mvar name mvar_tables
    case table of
        Left err -> return $ Left err
        Right table -> do
            let dFrame = snd table
            let modifiedDataFrame = updateFunction dFrame updates conditions
            let tableName = fst table
            res <- replaceTable_mvar (tableName, modifiedDataFrame) mvar_tables
            case res of
                Left err -> return $ Left err
                Right _ -> return $ Left "UPDATE"
-- INSERT
executeParsedStatement (InsertStatement name columns values) mvar_tables = do
    table <- getTable_mvar name mvar_tables
    case table of
        Left err -> return $ Left err
        Right table -> do
            let dFrame = snd table
            let modifiedDataFrame = insertFunction dFrame columns values
            let tableName = fst table
            res <- replaceTable_mvar (tableName, modifiedDataFrame) mvar_tables
            case res of
                Left err -> return $ Left err
                Right _ -> return $ Left "INSERT"
-- CREATE
executeParsedStatement (CreateTable name columns) mvar_tables = do
    db <- createTable_mvar name columns mvar_tables
    case db of
        Left err -> return $ Left err
        Right database -> do
            newTable <- getTable_mvar name mvar_tables
            case newTable of
                Left err -> return $ Left err
                Right newTable -> return $ Right (snd newTable)
-- DROP
executeParsedStatement (DropTable name) mvar_tables = do
    db <- dropTable_mvar name mvar_tables
    case db of
        Left err -> return $ Left err
        Right _ -> return $ Left "DROP TABLE"

    



replaceTable_mvar :: (TableName, DataFrame) -> MVar [(TableName, DataFrame)] -> IO (Either ErrorMessage [(TableName, DataFrame)])
replaceTable_mvar table mvar_db = do
    db <- takeMVar mvar_db
    let tables = replaceTable table db
    case tables of
        Left err -> return $ Left err
        Right df -> do
            putMVar mvar_db df
            return tables

replaceTable :: (TableName, DataFrame) -> [(TableName, DataFrame)] -> Either ErrorMessage [(TableName, DataFrame)]
replaceTable (tName, dFrame) db = do
    case dropTable tName db of
        Left err -> Left err
        Right tables -> Right $ (tName, dFrame) : tables 

instance ToJSON Value where
    toJSON (StringValue s) = object ["StringValue" .= s]
    toJSON (IntegerValue i) = object ["IntegerValue" .= i]
    toJSON (BoolValue b) = object ["BoolValue" .= b]
    toJSON NullValue = object ["NullValue" .= Null]

instance ToJSON Column where
    toJSON (Column name IntegerType) = object ["IntegerType" .= name]
    toJSON (Column name StringType) = object ["StringType" .= name]
    toJSON (Column name BoolType) = object ["BoolType" .= name]

instance ToJSON ColumnType where
    toJSON (IntegerType) = "IntegerType"
    toJSON (StringType) = "StringType"
    toJSON (BoolType) = "BoolType"

-- instance FromJSON ColumnType where
--     parseJSON (String "IntegerType") = return IntegerType
--     parseJSON (String "StringType")  = return StringType
--     parseJSON (String "BoolType")    = return BoolType
--     parseJSON _                   = fail "Invalid ColumnType"

instance ToJSON DataFrame where
    toJSON (DataFrame columns rows) = object ["columns" .= columns, "rows" .= rows]


instance ToJSON ParsedStatement where
    toJSON (SelectStatement columns name conditions order) = object ["columns" .= columns, "SelectStatement" .= name, "conditions" .= conditions, "order" .= order]
    toJSON (DeleteStatement name conditions) = object ["DeleteStatement" .= name, "conditions" .= conditions]
    toJSON (UpdateStatement name values conditions) = object ["UpdateStatement" .= name, "values" .= values, "conditions" .= conditions]
    toJSON (InsertStatement name columns values) = object ["InsertStatement" .= name, "columns" .= columns, "values" .= values]
    toJSON (CreateTable name columns) = object ["CreateTable" .= name, "columns" .= columns]
    toJSON (DropTable name) = object ["DropTable" .= name]

instance FromJSON ParsedStatement where
    parseJSON (Object v) = 
        (SelectStatement <$> v .: "columns" <*> v .: "SelectStatement" <*> v .: "conditions" <*> v .: "order")
        <|> (DeleteStatement <$> v .: "DeleteStatement" <*> v .: "conditions")
        <|> (UpdateStatement <$> v .: "UpdateStatement" <*> v .: "values" <*> v .: "conditions")
        <|> (InsertStatement <$> v .: "InsertStatement" <*> v .: "columns" <*> v .: "values")
        <|> (CreateTable <$> v .: "CreateTable" <*> v .: "columns")
        <|> (DropTable <$> v .: "DropTable")
    parseJSON _ = fail "Failed to parse Value"


valuesList :: [Value]
valuesList = [IntegerValue 5, StringValue "a", BoolValue True, NullValue]

columnList :: [Column]
columnList = [Column "a" IntegerType, Column "b" StringType, Column "c" BoolType, Column "d" IntegerType]

testData :: DataFrame
testData = DataFrame columnList [valuesList, valuesList]

testFunc :: BS.ByteString
testFunc = (encode (CreateTable "haha" [("aaa", IntegerType), ("bbb", StringType)]))

testFunc2 :: BS.ByteString
testFunc2 = (encode (SelectStatement ["a", "b", "c"] ["haha", "hhhh"] ["a=1"] [("a", True), ("b", False)]))

testFunc3 :: Either ParseException ParsedStatement
testFunc3 = decodeEither' (BS.pack "CreateTable: haha\ncolumns:\n- - aaa\n  - IntegerType\n- - bbb\n  - StringType\n") :: Either ParseException ParsedStatement

testFunc4 :: Either ParseException ParsedStatement
testFunc4 = decodeEither' (BS.pack "SelectStatement:\n- haha\n- hhhh\ncolumns:\n- a\n- b\n- c\nconditions:\n- a=1\norder:\n- - a\n  - true\n- - b\n  - false\n") :: Either ParseException ParsedStatement


-- LOAD MODIFY SAVE TABLES --------------------------------

type ErrorMessage = String
type Table = (TableName, DataFrame)

-- Thread for periodically saving datavase
thread_saveTables :: MVar [(TableName, DataFrame)] -> IO ()
thread_saveTables mvar_tables = forever $ do -- forever $ do
    threadDelay 1000000 -- Save delay
    putStrLn "Saving tables..."
    tables <- takeMVar mvar_tables
    saveTablesToDB tables
    writeFileLines (dbDirectory++"TABLELIST.txt") (map fst tables)
    putMVar mvar_tables tables
    putStrLn "Saved tables."
    where
        -- Function to write a list of strings to a file
        writeFileLines :: String -> [String] -> IO ()
        writeFileLines filePath linesList = do
            let contents = unlines linesList -- Join lines into a single string with newline characters
            writeFile filePath contents





getMultipleTables_mvar :: [TableName] -> MVar [(TableName, DataFrame)] -> IO (Either ErrorMessage [(TableName, DataFrame)])
getMultipleTables_mvar tNames mvar_db = do
    db <- takeMVar mvar_db
    let tables = getMultipleTables tNames db
    putMVar mvar_db db
    return tables


getMultipleTables :: [TableName] -> [(TableName, DataFrame)] -> Either ErrorMessage [(TableName, DataFrame)]
getMultipleTables [] _ = return []
getMultipleTables (tName : tNames) db = do
    current <- getTable tName db
    rest <- getMultipleTables tNames db
    return $ current : rest


-- getTable
getTable_mvar :: TableName -> MVar [(TableName, DataFrame)] -> IO (Either ErrorMessage (TableName, DataFrame))
getTable_mvar tName mvar_db = do
    db <- takeMVar mvar_db
    let table = getTable tName db
    putMVar mvar_db db
    return table


-- CREATE TABLE
-- Create Table with Column Info got from ParsingParsec.parseSql & update the MVar database
createTable_mvar :: TableName -> [(String, ColumnType)] -> MVar [(TableName, DataFrame)] -> IO (Either ErrorMessage [(TableName, DataFrame)])
createTable_mvar tName columnInfo mvar_db = do
    db <- takeMVar mvar_db
    let newDB = createTable tName columnInfo db
    case newDB of 
        Left err -> do
            putMVar mvar_db db
            putStrLn err
            return $ Left err
        Right newDB -> do
            putMVar mvar_db newDB
            return $ Right newDB

-- DROP TABLE 
-- Remove the given table from the MVar database
dropTable_mvar :: TableName -> MVar [(TableName, DataFrame)] -> IO (Either ErrorMessage [(TableName, DataFrame)])
dropTable_mvar tName mvar_db = do
    db <- takeMVar mvar_db
    let newDB = dropTable tName db
    case newDB of 
        Left err -> do
            putMVar mvar_db db
            putStrLn err
            return $ Left err
        Right newDB -> do
            putMVar mvar_db newDB
            return $ Right newDB

-- Same functions as before but don't use MVar & require
getTable :: TableName -> [(TableName, DataFrame)] -> Either ErrorMessage (TableName, DataFrame)
getTable tName [] = Left $ "Could not find table: '"++tName++"'!"
getTable tName (x : xs) = 
    if tName == fst x then 
        return  x
    else getTable tName xs

createTable :: TableName -> [(String, ColumnType)] -> [(TableName, DataFrame)] -> Either ErrorMessage [(TableName, DataFrame)]
createTable tName columnInfo db = 
    case getTable tName db of 
        Right _ -> Left $ "Table '"++tName++"' already exists!"
        Left _ -> do
            table <- constructTable tName columnInfo 
            return $ table : db
    where
    constructTable :: TableName -> [(String, ColumnType)] -> Either ErrorMessage (TableName, DataFrame)
    constructTable tableName columnInfo = do
        createDataFrame columnInfo >>= \df -> Right (tableName, df)
        where
            createDataFrame :: [(String, ColumnType)] -> Either ErrorMessage DataFrame
            createDataFrame columnInfo = Right $ DataFrame (createColumns columnInfo) []
            createColumns :: [(String, ColumnType)] -> [Column]
            createColumns [] = []
            createColumns ((columnName, columnType) : xs) = 
                (Column columnName columnType) : createColumns xs

dropTable :: TableName -> [(TableName, DataFrame)] -> Either ErrorMessage [(TableName, DataFrame)]
dropTable tName db = 
    case getTable tName db of 
        Left err -> Left $ "Could not delete table '"++tName++"'!"
        Right _ -> return $ filter (\(x,y) -> x /= tName) db








    
-- READ all tables that are in DB in "TABLELIST.txt"
loadTableListFromDB :: IO [String]
loadTableListFromDB = do
    readFileLines (dbDirectory ++ "TABLELIST.txt")
    where
        -- Function to read a file line by line and return a list of strings
        readFileLines :: String -> IO [String]
        readFileLines filePath = do
            contents <- readFile filePath
            return (lines contents)
-- LOAD TABLES FROM DB
loadTablesFromDB :: [String] -> IO (Either ErrorMessage [(TableName, DataFrame)])
loadTablesFromDB fileNames = do
    jsons <- readMultipleJSONFiles fileNames
    case jsonListToTableList jsons of 
        Left err -> return $ Left err
        Right dFrames -> return $ Right (zip fileNames dFrames)
-- LIB3 functions
-- Reading from multiple json Files
readMultipleJSONFiles :: [String] -> IO [String]
readMultipleJSONFiles [] = return []
readMultipleJSONFiles (x : xs) = do 
  fileContent <- readFile (dbDirectory ++ x ++ ".json")
  rest <- readMultipleJSONFiles xs
  return $ fileContent : rest
-- Parse a list of jsons to a list of DataFrames
jsonListToTableList :: [String] -> Either ErrorMessage [DataFrame]
jsonListToTableList [] = Right []
jsonListToTableList (x : xs) = 
  case jsonToDataframe x of 
    Just dFrame -> do 
      -- rest <- 
      case jsonListToTableList xs of 
        Right rest' -> 
          return $ (dFrame : rest')
        Left err -> Left err
    Nothing -> Left $ "Unable to parse DataFrame: " ++ x


-- Saving to multiple json Files
saveTablesToDB :: [(TableName, DataFrame)] -> IO ()
saveTablesToDB [] = return ()
saveTablesToDB tables = do
    saveMultipleJSONFiles (map fst tables) (map (\x -> dataframeToJson (snd x)) tables)

saveMultipleJSONFiles :: [TableName] -> [String] -> IO ()
saveMultipleJSONFiles [] _ = return ()
saveMultipleJSONFiles _ [] = return ()
saveMultipleJSONFiles (tName : tNames) (jsonContent : jsonContents) = do
    writeFile (dbDirectory ++ tName ++ ".json") jsonContent
    rest <- saveMultipleJSONFiles tNames jsonContents
    return $ rest

dbDirectory :: String
dbDirectory = "db/"