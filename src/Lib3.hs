{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE OverloadedStrings #-}
{-# OPTIONS_GHC -Wno-incomplete-patterns #-}

module Lib3
  ( executeSql,
    Execution,
    ExecutionAlgebra (..),
    runExecuteIO,
    isTestEnv,
  )
where

import Lib2 (capitalize)

import Control.Monad.Free (Free (..), liftF)
import Data.Time (UTCTime)
import Data.Time.Clock (getCurrentTime)
import Data.Time.Format (formatTime, defaultTimeLocale)
import DataFrame (Column (..), ColumnType (..), Value (..), Row, DataFrame (..))
import Text.Parsec
    ( alphaNum,
      char,
      space,
      spaces,
      string,
      choice,
      many1,
      option,
      sepBy1,
      (<?>),
      -- (<|>),
      parse,
      try )
import Text.Parsec.String ( Parser )
import Data.Char ( toLower, toUpper )
import System.IO
import Lib1 ( renderDataFrameAsTable )
import Lib2 (showTables, showTableByName, getTable, mergeListOfDataFrames, filterDataframe, columnListDF)
import Control.Exception (handle, IOException)
import System.Environment
import InMemoryTables (database)
import qualified Data.Text as T
import qualified Data.Text.Encoding as TE
import qualified Data.ByteString.Lazy.Char8 as BSL
import Data.Aeson as A hiding (Value)
import Control.Applicative ((<|>))

-- Keep the type, modify constructors
data ParsedStatement
  -- Condition = Condition(OR(int))
  = SelectStatement [String] [TableName] [String] -- [Column] [Table] [Condition]
  | DeleteStatement TableName [String] -- Table [Condition]
  | UpdateStatement TableName [String] [String] -- Table [column=newValue] [Condition]
  | InsertStatement TableName [String] [String] -- Table [Column] [Value]

  | ShowTables
  | ShowTableName String -- Table
  | ShowCurrentTime -- NOW()
  deriving (Show, Eq)

type TableName = String

type FileContent = String

type ErrorMessage = String

data ExecutionAlgebra next
  = LoadFile TableName ((Either ErrorMessage DataFrame) -> next)
  | LoadFileMultiple [TableName] ((Either ErrorMessage DataFrame) -> next)
  | GetTime (UTCTime -> next)
  -- feel free to add more constructors here
  | SaveFile TableName DataFrame ((Either ErrorMessage DataFrame) -> next)

  deriving (Functor)
  
-- Show instance for ExecutionAlgebra
instance Show (ExecutionAlgebra a) where
  show (LoadFile tableName _) = "LoadFile " ++ show tableName
  show (GetTime _) = "GetTime"
  show (SaveFile tableName _ _) = "SaveFile " ++ show tableName

type Execution = Free ExecutionAlgebra 



loadFile :: TableName -> Execution (Either ErrorMessage DataFrame)
loadFile name = liftF $ LoadFile name id

loadFileMultiple :: [TableName] -> Execution (Either ErrorMessage DataFrame)
loadFileMultiple names = liftF $ LoadFileMultiple names id

getTime :: Execution UTCTime
getTime = liftF $ GetTime id

saveFile :: TableName -> DataFrame -> Execution (Either ErrorMessage DataFrame)
saveFile name content = liftF $ SaveFile name content id 





-- /////////////////////// !!! MAIN(original) FUNCTION !!! ///////////////////////
executeSql :: String -> Execution (Either ErrorMessage DataFrame)
executeSql sql = do
  case parseSql sql of
    Left err -> return $ Left err
    Right parsedStatement -> executeParsedStatement parsedStatement




-- /////////////////////// !!! PARSED STATEMENT EXECUTION !!! ///////////////////////
executeParsedStatement :: ParsedStatement -> Execution (Either ErrorMessage DataFrame)
-- SELECT 
executeParsedStatement (SelectStatement columns tableNames conditions) = do
  -- TODO: Need to write InMemoryTables serialization to JSON
  dFrame <- loadFileMultiple tableNames
  -- let maybeFilteredDataframe = filterDataframe dFrame conditions
  case dFrame of
    Left err -> return $ Left err
    Right dFrame -> do
      case filterDataframe dFrame conditions of
        Just filteredDataframe -> do
          let selectedColumns = columnListDF columns filteredDataframe
          case selectedColumns of
            Right selectResult -> return $ Right selectResult
            Left _ -> return $ Left "Some columns were incorrect"
        Nothing -> return $ Left "Incorrect condition"
  
-- DELETE (cia)
executeParsedStatement (DeleteStatement tableName conditions) = do
  let tableName = "example" -- TEMP
  dFrame <- loadFile tableName
  case dFrame of 
    Right dFrame -> do
      let modifiedDataFrame = dFrame -- DELETE
      savedFile <- saveFile tableName (modifiedDataFrame) -- failo rasymas vyksta: runStep (SaveFile tableName fileContent next)
      return $ Right modifiedDataFrame 
    Left err -> return $ Left err

-- UPDATE
executeParsedStatement (UpdateStatement tableName updates conditions) = do
  let tableName = "example" -- TEMP
  dFrame <- loadFile tableName
  case dFrame of 
    Right dFrame -> do
      let modifiedDataFrame = dFrame -- UPDATE
      savedFile <- saveFile tableName (modifiedDataFrame) -- failo rasymas vyksta: runStep (SaveFile tableName fileContent next)
      return $ Right modifiedDataFrame 
    Left err -> return $ Left err
-- INSERT
executeParsedStatement (InsertStatement tableName columns values) = do
  let tableName = "example" -- TEMP
  dFrame <- loadFile tableName
  case dFrame of 
    Right dFrame -> do
      let modifiedDataFrame = dFrame -- INSERT
      savedFile <- saveFile tableName (modifiedDataFrame) -- failo rasymas vyksta: runStep (SaveFile tableName fileContent next)
      return $ Right modifiedDataFrame 
    Left err -> return $ Left err

-- SHOW TABLES
executeParsedStatement (ShowTableName tableName) = do
  return $ (Lib2.showTableByName tableName)
executeParsedStatement (ShowTables) = do
  return $ Right Lib2.showTables
-- NOW()
executeParsedStatement (ShowCurrentTime) = do
  currentTime <- getTime
  let formattedTime = formatTime defaultTimeLocale "%Y-%m-%d %H:%M:%S" currentTime
  let dataFrameWithTime = DataFrame [Column formattedTime StringType] []
  return $ Right dataFrameWithTime

executeParsedStatement a = do
  return $ Left ("Constructor: " ++ show a ++ " is not supported.")




--- /////////////////// FROM Main.hs ///////////////////

-- BASICALLY THE cmd FUNCTION FROM (Main.hs) (for testing)
f :: String -> IO (Either String String)
f statement = do
  df <- runExecuteIO $ Lib3.executeSql statement 
  return $ Lib1.renderDataFrameAsTable 100 <$> df


-- Decide which interpreter to use
runExecuteIO :: Lib3.Execution r -> IO r
runExecuteIO execution = do 
  isTest <- isTestEnv
  if isTest then testInterpreter execution
  else productionInterpreter execution

-- Shared actions between the TEST & PRODUCTION interpreters
runStepSHARED :: Lib3.ExecutionAlgebra r -> IO r
runStepSHARED (Lib3.GetTime next) = getCurrentTime >>= return . next

-- /////////////////// PRODUCTION INTERPRETER ///////////////////

-- Production Interpreter
productionInterpreter :: Lib3.Execution r -> IO r
productionInterpreter (Pure r) = return r
productionInterpreter (Free step) = do
  next <- runStep step
  productionInterpreter next
    where 
      -- LoadFile
      runStep :: Lib3.ExecutionAlgebra a -> IO a
      runStep (LoadFile tableName next) = do 
        -- putStrLn $ "LoadFile: " ++ ("db/" ++ tableName ++ ".json")
        fileContent <- readFile ("db/" ++ tableName ++ ".json")
        case jsonToDataframe fileContent of 
          Just dFrame -> return $ next (Right dFrame)
          Nothing -> return $ next (Left $ "Could not load table \"" ++ tableName ++ "\"")
      -- LoadFileMultiple
      runStep (LoadFileMultiple tableNames next) = do 
        -- putStrLn $ "Opening multiple files: " ++ show tableNames
        jsons <- readMultipleJSONFiles tableNames
        case jsonListToDataFrameList jsons of
          Left err -> return $ next (Left err)
          Right dFrames -> do
            let bigDFrame = connectTables dFrames
            -- putStrLn $ "BigDframe: \n" ++ (renderDataFrameAsTable 100 bigDFrame)
            return $ next $ Right bigDFrame

      -- SaveFile
      runStep (SaveFile tableName dFrame next) = do
        let fileContent = dataframeToJson dFrame
        -- putStrLn $ "SaveTable: " ++ show tableName ++ " with content:" ++ "\n" ++ show dFrame
        withFile ("db/" ++ tableName ++ ".json") WriteMode (\handle -> hPutStr handle fileContent)
        return $ next (Right dFrame)
      runStep execution = runStepSHARED execution


connectTables :: [DataFrame] -> DataFrame
connectTables [] = error "No specified tables"
connectTables (df:dfs) = do 
  foldl combineDataFrames df dfs
  where
    combineDataFrames :: DataFrame -> DataFrame -> DataFrame
    combineDataFrames (DataFrame cols1 rows1) (DataFrame cols2 rows2) =
      DataFrame (cols1 ++ cols2) [row1 ++ row2 | row1 <- rows1, row2 <- take (length cols2) rows2]

-- Reading from multiple json Files
readMultipleJSONFiles :: [String] -> IO [String]
readMultipleJSONFiles [] = return []
readMultipleJSONFiles (x : xs) = do 
  fileContent <- readFile ("db/" ++ x ++ ".json")
  rest <- readMultipleJSONFiles xs
  return $ fileContent : rest

-- Parse a list of jsons to a list of DataFrames
jsonListToDataFrameList :: [String] -> Either ErrorMessage [DataFrame]
jsonListToDataFrameList [] = Right []
jsonListToDataFrameList (x : xs) = 
  case jsonToDataframe x of 
    Just dFrame -> do 
      -- rest <- 
      case jsonListToDataFrameList xs of 
        Right rest' -> 
          return $  (dFrame : rest')
        Left err -> Left err
    Nothing -> Left $ "Unable to parse DataFrame: " ++ x




-- /////////////////// TEST INTERPRETER ///////////////////

-- Test Interpretere
testInterpreter :: Lib3.Execution r -> IO r
testInterpreter (Pure r) = return r
testInterpreter (Free step) = do
  next <- runStepTEST step
  testInterpreter next
    where
      -- LoadFile
      runStepTEST :: Lib3.ExecutionAlgebra a -> IO a
      runStepTEST (LoadFile tableName next) = do 
        -- putStrLn $ "TEST: LoadFile: " ++ tableName
        case getTable database tableName of 
          Just dFrame -> return $ next (Right dFrame) -- into JSON and return
          Nothing -> return $ next (Left $ "Table " ++ tableName ++ " not found.")
      -- LoadFileMultiple
      runStepTEST (LoadFileMultiple tableNames next) = do 
        case getTablesFromInMemoryTables tableNames of 
          Left err -> return $ next $ Left err
          Right dFrames -> do
            let bigDFrame = connectTables dFrames
            -- putStrLn $ "BigDframe: \n" ++ (renderDataFrameAsTable 100 bigDFrame)
            return $ next $ Right bigDFrame
      -- SaveFile
      runStepTEST (SaveFile tableName dFrame next) = do
        -- putStrLn $ "TEST: SaveTable: " ++ show tableName ++ " with content:" ++ "\n" ++ show dFrame
        case getTable database tableName of 
          Just dFrame -> return $ next (Right dFrame) -- from JSON and return
          Nothing -> return $ next (Left $ "Could not save table \"" ++ tableName ++ "\"")
      runStepTEST execution = runStepSHARED execution

-- Gets the given tables from "InMemoryTables.hs"
getTablesFromInMemoryTables :: [TableName] -> Either ErrorMessage [DataFrame]
getTablesFromInMemoryTables [] = Right []
getTablesFromInMemoryTables (x : xs) = 
  case getTable database x of 
    Nothing -> Left $ "Table: \"" ++ x ++ "\" does not exist in the database."
    Just table -> do
      rest <- getTablesFromInMemoryTables xs
      return $ table : rest









-- Checks if currently testing
isTestEnv :: IO Bool
isTestEnv = do
  isTesting <- lookupEnv "ENVIRONMENT_TEST"
  case isTesting of
    Just _ -> do
      return True
    Nothing -> do
      return False







-- TEST DATA

a = Column "a" IntegerType
b = Column "b" IntegerType
c = Column "c" StringType
d = Column "d" BoolType
row1 = [IntegerValue 5, IntegerValue 15, StringValue "ddddddddddd", BoolValue True]
row2 :: [Value]
row2 = [IntegerValue 10, IntegerValue 20, StringValue "no", BoolValue False]
testData :: DataFrame
testData = DataFrame [a, b, c, d] [row1, row2]





-- //////////////////////////// WRITE/READ JSON ////////////////////////////

instance FromJSON ColumnType where
  parseJSON (String "IntegerType") = return IntegerType
  parseJSON (String "StringType")  = return StringType
  parseJSON (String "BoolType")    = return BoolType
  parseJSON _                   = fail "Invalid ColumnType"

instance FromJSON Column where
  parseJSON (Object v) = do
    name <- v .: "name"
    colType <- v .: "type"
    return $ Column name colType
  parseJSON _ = fail "Invalid Column"

instance FromJSON Value where
  parseJSON (Object v) = IntegerValue <$> v .: "IntegerValue"
    <|> (StringValue <$> v .: "StringValue")
    <|> (BoolValue <$> v .: "BoolValue")
    <|> pure NullValue
  parseJSON _ = fail "Failed to parse Value"

instance FromJSON DataFrame where
  parseJSON (Object v) = do
    columns <- v .: "columns"
    rows <- v .: "rows"
    return $ DataFrame columns rows
  parseJSON _ = fail "Invalid DataFrame"


jsonToDataframe :: String -> Maybe DataFrame
jsonToDataframe jsonData = decode (BSL.fromStrict $ TE.encodeUtf8 $ T.pack jsonData)

writeToTestFile :: DataFrame -> IO()
writeToTestFile dataframe = writeFile "db/jsonTesting.json" (dataframeToJson dataframe)

--DATAFRAME WRITING
dataframeToJson :: DataFrame -> String
dataframeToJson (DataFrame columns rows) = "{\"columns\": " ++ columnsToJsonStart columns ++ ", \"rows\": " ++ rowsToJsonStart rows ++ "}"


--COLUMN WRITING
columnToJson :: Column -> String
columnToJson (Column name colType) = "{\"name\": " ++ show name ++ ", \"type\": " ++ show (show colType) ++ "}"

columnsToJsonStart :: [Column] -> String
columnsToJsonStart (x:xs) = "[" ++ columnToJson x ++ columnsToJson xs ++ "]"

columnsToJson :: [Column] -> String
columnsToJson (x:xs) = ", " ++ columnToJson x ++ columnsToJson xs
columnsToJson [] = ""

--ROW WRITING
rowsToJsonStart :: [Row] -> String
rowsToJsonStart (x:xs) = "[" ++ rowToJsonStart x ++ rowsToJson xs ++ "]"

rowsToJson :: [Row] -> String
rowsToJson (x:xs) = ", " ++ rowToJsonStart x ++ rowsToJson xs
rowsToJson [] = ""

rowToJsonStart :: Row -> String
rowToJsonStart (x:xs) = "[" ++ valueToJson x ++ rowToJson xs ++ "]"

rowToJson :: Row -> String
rowToJson (x:xs) = ", " ++ valueToJson x ++ rowToJson xs
rowToJson [] = ""

valueToJson :: Value -> String
valueToJson (IntegerValue value) = "{\"IntegerValue\": " ++ show value ++ "}"
valueToJson (StringValue value) = "{\"StringValue\": " ++ show value ++ "}"
valueToJson (BoolValue value) = if value then "{\"BoolValue\": true}" else "{\"BoolValue\": false}"
valueToJson NullValue = "{\"NullValue\": null}"






-- ////////////////////////////  SQL PARSING ////////////////////////////

-- String to SQL statement parser(takes string and returns error or the corresponding sql statement constructor)
parseSql :: String -> Either ErrorMessage (ParsedStatement)
parseSql input = case parse sqlParser "" input of
  Left err -> Left $ "Parse error at " ++ show err
  Right result -> Right result

-- SQL statement parser (parses all statements)
sqlParser :: Parser ParsedStatement
sqlParser = do
  spaces
  choice [selectParser, deleteParser, insertParser, updateParser, try showTablesParser <|> showTableNameParser, nowParser] 

-- SELECT statement parser
selectParser :: Parser ParsedStatement
selectParser = do
  spaces
  caseInsensitiveString "SELECT"
  spaces
  columns <- commaSepListParser
  caseInsensitiveString "FROM"
  spaces
  tables <- commaSepListParser
  spaces
  conditions <- option [] whereParser
  spaces
  string ";"
  return $ SelectStatement columns tables conditions

-- DELETE statement parser
deleteParser :: Parser ParsedStatement
deleteParser = do
  spaces
  caseInsensitiveString "DELETE"
  spaces
  caseInsensitiveString "FROM"
  spaces
  table <- identifierParser
  spaces
  conditions <- option [] whereParser
  spaces
  string ";"
  return $ DeleteStatement table conditions

-- INSERT statement parser
insertParser :: Parser ParsedStatement
insertParser = do
  spaces
  caseInsensitiveString "INSERT"
  spaces
  caseInsensitiveString "INTO"
  spaces
  table <- identifierParser
  spaces
  string "("
  columns <- commaSepListParser
  string ")"
  spaces
  values <- valuesParser
  spaces
  string ";"
  return $ InsertStatement table columns values
    where
      valuesParser :: Parser [String] 
      valuesParser = do
        caseInsensitiveString "VALUES"
        spaces
        string "("
        values <- commaSepListParser
        string ")"
        spaces
        return values

-- UPDATE statement parser
updateParser :: Parser ParsedStatement
updateParser = do
  spaces
  caseInsensitiveString "UPDATE"
  spaces
  table <- identifierParser
  updates <- setParser
  conditions <- option [] whereParser
  spaces
  string ";"
  return $ UpdateStatement table updates conditions

-- NOW() statement parser
nowParser :: Parser ParsedStatement
nowParser = do
  spaces
  caseInsensitiveString "NOW()"
  spaces
  string ";"
  return $ ShowCurrentTime

-- SHOW TABLES statement parser
showTablesParser :: Parser ParsedStatement
showTablesParser = do
  spaces
  caseInsensitiveString "SHOW"
  spaces
  caseInsensitiveString "TABLES"
  spaces
  string ";"
  return ShowTables

-- SHOW TABLE name statement parser
showTableNameParser :: Parser ParsedStatement
showTableNameParser = do
  spaces
  caseInsensitiveString "SHOW"
  spaces
  caseInsensitiveString "TABLE"
  space
  table <- many1 (alphaNum <|> char '_')
  spaces
  string ";"
  return $ ShowTableName table


-- ///// Parsing Helper functions
-- Parse a list of words separated by ',' (columns, tableNames)
commaSepListParser :: Parser [String]
commaSepListParser = do
  spaces
  identifiers <- sepBy1 identifierParser (char ',')
  return $ identifiers

-- Parse a single identifier (column or table name)
identifierParser :: Parser String
identifierParser = do
  spaces
  items <- many1 (alphaNum <|> char '_' <|> char '.') <|> (string "*")
  spaces
  return items

-- WHERE part parser (for SELECT, DELETE, UPDATE)
whereParser :: Parser [String]
whereParser = do
  spaces
  caseInsensitiveString "WHERE"
  spaces
  conditions <- whereOrParser
  return conditions
whereOrParser :: Parser [String]
whereOrParser = do
  spaces
  conditions <- sepBy1 conditionParser (caseInsensitiveString "OR")
  return $ conditions
conditionParser :: Parser String
conditionParser = do
  spaces
  conditionLeft <- try $ many1 (alphaNum <|> char '_' <|> char '.')
  spaces
  operator <- many1 (char '>' <|> char '<' <|> char '=' <|> char '!')
  spaces
  conditionRight <- many1 (alphaNum <|> char '_' <|> char '.')
  spaces
  return $ conditionLeft ++ operator ++ conditionRight

-- SET part parser (for UPDATE)
setParser :: Parser [String]
setParser = do
  spaces
  caseInsensitiveString "SET"
  spaces
  spaces
  updates <- sepBy1 singleUpdateParser (char ',')
  spaces
  return $ updates
-- Parser for each SET attribute
singleUpdateParser :: Parser String
singleUpdateParser = do
  spaces
  column <- identifierParser
  spaces
  string "="
  spaces
  newValue <- identifierParser
  spaces
  return $ column ++ "=" ++ newValue

-- Parser for case insensitive string
-- Match the lowercase or uppercase form of 'c'
caseInsensitiveChar c = char (toLower c) <|> char (toUpper c)
-- Match the string 's', accepting either lowercase or uppercase form of each character 
caseInsensitiveString s = try (mapM caseInsensitiveChar s) Text.Parsec.<?> "\"" ++ s ++ "\""

-- //////////////////////////// END OF PARSING ////////////////////////////


