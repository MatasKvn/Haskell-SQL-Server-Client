{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE OverloadedStrings #-}
{-# OPTIONS_GHC -Wno-incomplete-patterns #-}
{-# OPTIONS_GHC -Wno-unrecognised-pragmas #-}
{-# HLINT ignore "Use camelCase" #-}

module LoadModifySaveTables where

import Control.Concurrent
import DataFrame
import Lib3
import InMemoryTables (TableName)
import DataFrame (DataFrame (DataFrame), Column (Column), ColumnType(..))
import Lib1 (renderDataFrameAsTable)


type ErrorMessage = String
type Table = (TableName, DataFrame)



-- Examples for code usage
main :: IO ()
main = do
    -- let inDBTables = ["employees", "flags"] -- TEMP
    inDBTables <- loadTableListFromDB
    _loadedTables <- loadTablesFromDB inDBTables
    case _loadedTables of 
        Left err -> do 
            putStrLn err
        Right loadedTables -> do
            mvar_tables <- newMVar loadedTables

            -- TESTING table create & drop
            createTable_mvar "employees" [("StringColumn 1", StringType),("IntegerColumn 2", IntegerType),("BoolColumn 3", BoolType)] mvar_tables

            createTable_mvar "idk" [("Column1", StringType)] mvar_tables
            tables <- takeMVar mvar_tables
            putStrLn $ show (map fst tables)
            putMVar mvar_tables tables

            dropTable_mvar "laskdjflkj" mvar_tables

            dropTable_mvar "idk" mvar_tables
            tables <- takeMVar mvar_tables
            putStrLn $ show (map fst tables)
            putMVar mvar_tables tables


                
            -- TODO: fix json parsing from empty string
            -- ghc-9.4.5.exe: D:\\Universitetas\.Funkcinis\FunkcinisProgramavimas\src\Lib3.hs:377:1-72: Non-exhaustive patterns in function rowsToJsonStart        

            -- -- Start periodical saving thread (breaks because of mentioned bug)
            -- saveThreadID <- forkIO $ thread_saveTables mvar_tables 

            employeesTable <- getTable_mvar "employees" mvar_tables
            case employeesTable of 
                Left err -> putStrLn err
                Right (tName, dFrame) -> putStrLn $ renderDataFrameAsTable 100 dFrame

            return ()       







-- Thread for periodically saving datavase
thread_saveTables :: MVar [(TableName, DataFrame)] -> IO ()
thread_saveTables mvar_tables = do -- forever $ do
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
dbDirectory = "dbTemp/"