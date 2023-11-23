{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE DeriveFunctor #-}

module Lib3
  ( executeSql,
    Execution,
    ExecutionAlgebra (..),
  )
where

import Lib2 (capitalize, parseStatement)

import Control.Monad.Free (Free (..), liftF)
import Data.Time (UTCTime)
import DataFrame (DataFrame)
import Text.Parsec
import Text.Parsec.String
import Text.Parsec.Error (ParseError, errorMessages, messageString)
import Data.Char


type TableName = String

type FileContent = String

type ErrorMessage = String

data ExecutionAlgebra next
  = LoadFile TableName (FileContent -> next)
  | GetTime (UTCTime -> next)
  -- feel free to add more constructors here
  -- Condition = Condition(OR(int))
  | SelectStatement [String] [String] [String] -- [Column] [Table] [Condition]
  | DeleteStatement String [String] -- Table [Condition]
  | UpdateStatement String [String] [String] -- Table [column=newValue] [Condition]
  | InsertStatement String [String] [String] -- Table [Column] [Value]

  | ShowTables
  | ShowTableName String -- Table
  | ShowCurrentTime -- NOW()

  deriving (Functor)
  
-- Show instance for ExecutionAlgebra
instance Show (ExecutionAlgebra a) where
  show (LoadFile tableName _) = "LoadFile " ++ show tableName
  show (GetTime _) = "GetTime"
  show (SelectStatement columns tables conditions) = "SelectStatement " ++ show columns ++ show tables ++ show conditions
  show (DeleteStatement table conditions) = "DeleteStatement " ++ show table ++ show conditions
  show (InsertStatement table updates conditions) = "InstertStatement " ++ show table ++ show updates ++ show conditions
  show (UpdateStatement table columns conditions) = "UpdateStatement " ++ show table ++ show columns ++ show conditions
  show (ShowTables) = "ShowTables"
  show (ShowTableName table) = "ShowTableName " ++ show table
  show (ShowCurrentTime) = "ShowCurrentTime"

type Execution = Free ExecutionAlgebra

loadFile :: TableName -> Execution FileContent
loadFile name = liftF $ LoadFile name id

getTime :: Execution UTCTime
getTime = liftF $ GetTime id

executeSql :: String -> Execution (Either ErrorMessage DataFrame)
executeSql sql = do
  case parseSql sql of
    Left err -> return $ Left err
    Right parsedStatement -> executeParsedStatement parsedStatement
    
  -- executeParsedStatement parsedStatement
  -- return $ Left "implement me"

executeParsedStatement :: ExecutionAlgebra () -> Execution (Either ErrorMessage DataFrame)
executeParsedStatement statement = do
  -- Implement the logic to execute the parsed statement and return a DataFrame
  return $ Left $ "implement me" ++ "\nTried executing: " ++ show statement















-- //////////////////////////// PARSING ////////////////////////////

-- String to SQL statement parser(takes string and returns error or the corresponding sql statement constructor)
parseSql :: String -> Either ErrorMessage (ExecutionAlgebra ())
parseSql input = case parse sqlParser "" input of
  Left err -> Left $ "Parse error at " ++ show err
  Right result -> Right result

-- SQL statement parser (parses all statements)
sqlParser :: Parser (ExecutionAlgebra ()) 
sqlParser = do
  spaces
  choice [selectParser, deleteParser, insertParser, updateParser, try showTablesParser <|> showTableNameParser, nowParser] 

-- SELECT statement parser
selectParser :: Parser (ExecutionAlgebra ())
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
deleteParser :: Parser (ExecutionAlgebra ())
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
insertParser :: Parser (ExecutionAlgebra ())
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
updateParser :: Parser (ExecutionAlgebra ())
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
nowParser :: Parser (ExecutionAlgebra ())
nowParser = do
  spaces
  caseInsensitiveString "NOW()"
  spaces
  string ";"
  return $ ShowCurrentTime

-- SHOW TABLES statement parser
showTablesParser :: Parser (ExecutionAlgebra ())
showTablesParser = do
  spaces
  caseInsensitiveString "SHOW"
  spaces
  caseInsensitiveString "TABLES"
  spaces
  string ";"
  return ShowTables

-- SHOW TABLE name statement parser
showTableNameParser :: Parser (ExecutionAlgebra ())
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