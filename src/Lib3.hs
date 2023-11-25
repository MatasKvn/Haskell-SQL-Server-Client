{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE DeriveFunctor #-}

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
import Text.Parsec hiding (Column)
import Text.Parsec.String
import Text.Parsec.Error (ParseError, errorMessages, messageString)
import Data.Char
import System.IO
import qualified Lib1
import Lib2 (showTables, showTableByName, getTable)
import Control.Exception (handle, IOException)
import System.Environment
import InMemoryTables (database)



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
  = LoadFile TableName (FileContent -> next)
  | GetTime (UTCTime -> next)
  -- feel free to add more constructors here
  | SaveFile TableName FileContent (FileContent -> next)

  deriving (Functor)
  
-- Show instance for ExecutionAlgebra
instance Show (ExecutionAlgebra a) where
  show (LoadFile tableName _) = "LoadFile " ++ show tableName
  show (GetTime _) = "GetTime"
  show (SaveFile tableName _ _) = "SaveFile " ++ show tableName

type Execution = Free ExecutionAlgebra

loadFile :: TableName -> Execution FileContent
loadFile name = liftF $ LoadFile name id

getTime :: Execution UTCTime
getTime = liftF $ GetTime id

saveFile :: TableName -> FileContent -> Execution FileContent
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
  fileContent <- loadFile "db/example.txt" 
  let selectedDataFrame = (DataFrame [Column "SELECT statement" StringType] [ [StringValue "Not implemented"] ])
  return $ Right selectedDataFrame
-- DELETE (cia)
executeParsedStatement (DeleteStatement tableName conditions) = do
  fileContent <- loadFile "db/example.txt" -- failo nuskaitymas is tikruju vyksta: runStep (LoadFile tableName next)
  -- pakeist kad skaitytu atitinkamu lenteliu failus (.txt tik pavyzdziui, turi buti JSON)

  -- padaryt kad parsintu is JSONo i DataFrame
  let parsedDataFrame = DataFrame [Column "DELETE statement" StringType] [ [StringValue "Not implemented"] ]

  -- pritaikyti "DELETE" statementa
  let modifiedDataFrame = parsedDataFrame

  -- parsinti DataFrame i JSONa
  let modifiedfileContent = "DELETE statement" 

  -- saugoti isparsinta JSON texta i atitinkama faila
  savedFile <- saveFile "db/example.txt" (modifiedfileContent) -- failo rasymas vyksta: runStep (SaveFile tableName fileContent next)

  -- Grazinam modifikuota DataFrame isspausdinimui
  return $ Right modifiedDataFrame -- return modified dataFrame to print in console
-- UPDATE
executeParsedStatement (UpdateStatement tableName updates conditions) = do
  fileContent <- loadFile "db/example.txt"
  let modifiedDataFrame = (DataFrame [Column "UPDATE statement" StringType] [ [StringValue "Not implemented"] ])
  let modifiedfileContent = "UPDATE statement"
  savedFileContent <- saveFile "db/example.txt" (modifiedfileContent)
  return $ Right modifiedDataFrame
-- INSERT
executeParsedStatement (InsertStatement tableName columns values) = do
  fileContent <- loadFile "db/example.txt"
  let modifiedDataFrame = (DataFrame [Column "INSERT statement" StringType] [ [StringValue "Not implemented"] ])
  let modifiedfileContent = "INSERT statement"
  savedFileContent <- saveFile "db/example.txt" (modifiedfileContent)
  return $ Right modifiedDataFrame

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



runExecuteIO :: Lib3.Execution r -> IO r
runExecuteIO execution = do 
  isTest <- isTestEnv
  if isTest then testInterpreter execution
  else productionInterpreter execution


-- PRODUCTION INTERPRETER
productionInterpreter :: Lib3.Execution r -> IO r
productionInterpreter (Pure r) = return r
productionInterpreter (Free step) = do
  next <- runStep step
  productionInterpreter next
    where 
      -- LoadFile
      runStep :: Lib3.ExecutionAlgebra a -> IO a
      runStep (LoadFile tableName next) = do 
        putStrLn $ "LoadFile: " ++ tableName
        fileContent <- withFile tableName ReadMode hGetContents
        return $ next fileContent
      -- SaveFile
      runStep (SaveFile tableName fileContent next) = do
        putStrLn $ "SaveTable: " ++ show tableName ++ " with content:" ++ "\n" ++ fileContent
        withFile tableName WriteMode (\handle -> hPutStr handle fileContent)
        return $ next fileContent
      runStep (Lib3.GetTime next) = getCurrentTime >>= return . next




-- TEST INTERPRETER
testInterpreter :: Lib3.Execution r -> IO r
testInterpreter (Pure r) = return r
testInterpreter (Free step) = do
  next <- runStepTEST step
  testInterpreter next
    where
      -- LoadFile
      runStepTEST :: Lib3.ExecutionAlgebra a -> IO a
      runStepTEST (LoadFile tableName next) = do 
        putStrLn $ "TEST: LoadFile: " ++ tableName
        case getTable database tableName of 
          Just dFrame -> return $ next (show dFrame) -- into JSON and return
          Nothing -> return $ next ""
      -- SaveFile
      runStepTEST (SaveFile tableName fileContent next) = do
        putStrLn $ "TEST: SaveTable: " ++ show tableName ++ " with content:" ++ "\n" ++ fileContent
        case getTable database tableName of 
          Just dFrame -> return $ next (show dFrame) -- from JSON and return
          Nothing -> return $ next ""
      runStepTEST (Lib3.GetTime next) = getCurrentTime >>= return . next








--- /////////////////// FROM Main.hs ///////////////////
-- /////////////////////// !!! PARSED STATEMENT EXECUTION !!! ///////////////////////

-- CHECKS IF CURRENTLY TESTING
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


















-- //////////////////////////// PARSING ////////////////////////////

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
caseInsensitiveString s = try (mapM caseInsensitiveChar s) <?> "\"" ++ s ++ "\""

-- //////////////////////////// END OF PARSING ////////////////////////////




-- Redundant code

deleteExecution :: ParsedStatement -> Execution (Either ErrorMessage DataFrame)
deleteExecution (DeleteStatement tableName conditions) = do
  -- Read & Parse JSON here
  fileContent <- loadFile "db/example.txt" 
  -- Delete here
  let modifiedfileContent = if null fileContent then [] else tail fileContent -- example
  -- Encode to JSON & write
  savedFile <- saveFile "db/example.txt" (modifiedfileContent)
  return $ Right (DataFrame [] []) -- return modified dataFrame to print in console