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

  deriving (Functor)
-- Show instance for ExecutionAlgebra
instance Show (ExecutionAlgebra a) where
  show (LoadFile tableName _) = "LoadFile " ++ show tableName
  show (GetTime _) = "GetTime"
  show (SelectStatement columns tables conditions) = "SelectStatement " ++ show columns ++ show tables ++ show conditions
  show (DeleteStatement table conditions) = "DeleteStatement " ++ show table ++ show conditions
  show (InsertStatement table updates conditions) = "InstertStatement " ++ show table ++ show updates ++ show conditions
  show (UpdateStatement table columns conditions) = "UpdateStatement " ++ show table ++ show columns ++ show conditions

type Execution = Free ExecutionAlgebra



-- //////////////////////////// PARSING


parseSql :: String -> Either ErrorMessage (ExecutionAlgebra ())
parseSql input = case parse sqlParser "" input of
  Left err -> Left $ "Parse error at " ++ show err
  Right result -> Right result


sqlParser :: Parser (ExecutionAlgebra ()) 
sqlParser = choice [selectParser, deleteParser, insertParser, updateParser] 


-- SELECT
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

-- DELETE
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

-- INSERT
-- Table [Column] [Value]
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

-- UPDATE
-- Table [column=newValue] [Condition]
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


-- ///// Helpers
-- Parse a list of columns
commaSepListParser :: Parser [String]
commaSepListParser = do
  spaces
  identifiers <- sepBy1 identifierParser (char ',')
  return $ identifiers

-- Parse a single identifier (column or table name)
identifierParser :: Parser String
identifierParser = do
  spaces
  items <- many1 (alphaNum <|> char '_') <|> (string "*")
  spaces
  return items

-- Parses the "WHERE" part
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

-- Parse the "SET" part in "UPDATE" statement
setParser :: Parser [String]
setParser = do
  spaces
  caseInsensitiveString "SET"
  spaces
  spaces
  updates <- sepBy1 singleUpdateParser (char ',')
  spaces
  return $ updates
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


-- Match the lowercase or uppercase form of 'c'
caseInsensitiveChar c = char (toLower c) <|> char (toUpper c)
-- Match the string 's', accepting either lowercase or uppercase form of each character 
caseInsensitiveString s = try (mapM caseInsensitiveChar s) <?> "\"" ++ s ++ "\""

-- //////////////////////////// END OF PARSING


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