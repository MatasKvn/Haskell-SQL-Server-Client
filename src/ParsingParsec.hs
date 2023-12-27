{-# LANGUAGE FlexibleContexts #-}
{-# OPTIONS_GHC -Wno-unused-do-bind #-}

module ParsingParsec (
    ParsedStatement(..),
    parseSql,
    ) where

import Data.Char
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
      parse,
      try, sepBy )
import Text.Parsec.String ( Parser )      
import Control.Applicative ((<|>))
import DataFrame (ColumnType(..))

type ErrorMessage = String
type TableName = String

-- Keep the type, modify constructors
data ParsedStatement
  -- Condition = Condition(OR(int))
  = SelectStatement [String] [TableName] [String] [(String, Bool)] -- [Column] [Table] [Condition] [(Column, isAsc)]
  | DeleteStatement TableName [String] -- Table [Condition]
  | UpdateStatement TableName [String] [String] -- Table [column=newValue] [Condition]
  | InsertStatement TableName [String] [String] -- Table [Column] [Value]

  | CreateTable TableName [(String, ColumnType)] -- Table [(Column, ColumnType)]
  | DropTable TableName

  | ShowTables
  | ShowTableName TableName -- Table
  | ShowCurrentTime -- NOW()
  deriving (Show, Eq)



-- String to SQL statement parser(takes string and returns error or the corresponding sql statement constructor)
parseSql :: String -> Either ErrorMessage (ParsedStatement)
parseSql input = case parse sqlParser "" input of
  Left err -> Left $ "Parse error at " ++ show err
  Right result -> Right result

-- SQL statement parser (parses all statements)
sqlParser :: Parser ParsedStatement
sqlParser = do
  spaces
  choice [selectParser, deleteParser, insertParser, updateParser, try showTablesParser <|> showTableNameParser, nowParser, createTableParser, dropTableParser] 

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
  orderings <- option [] orderByParser
  spaces
  string ";"
  return $ SelectStatement columns tables conditions orderings

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




dropTableParser :: Parser ParsedStatement
dropTableParser = do
    spaces
    caseInsensitiveString "DROP"
    spaces
    caseInsensitiveString "TABLE"
    spaces
    table <- identifierParser
    spaces
    char ';'
    return $ DropTable table


createTableParser :: Parser ParsedStatement
createTableParser = do
    spaces
    caseInsensitiveString "CREATE"
    spaces
    caseInsensitiveString "TABLE"
    spaces
    table <- identifierParser
    spaces
    char '('
    spaces
    columnsAndTypes <- createColumnsParser
    spaces
    char ')'
    spaces
    char ';'
    return $ CreateTable table columnsAndTypes
    where createColumnsParser = sepBy createColumnParser (char ',')

-- (TableName, Column)
createColumnParser :: Parser (String, ColumnType)
createColumnParser = do
    spaces
    column <- identifierParser
    spaces
    columnType <- caseInsensitiveString "INT" <|> caseInsensitiveString "STRING" <|> caseInsensitiveString "BOOL" 
    spaces
    case capitalize columnType of
        "INT" -> return (column, IntegerType)
        "STRING" -> return (column, StringType)
        "BOOL" -> return (column, BoolType)
    where capitalize = map (\x -> toUpper x)
















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
  items <- many1 (alphaNum <|> char '_' <|> char '.' <|> char '\'') <|> (string "*")
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
  conditions <- sepBy1 conditionParser (caseInsensitiveString "OR ")
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
caseInsensitiveChar :: Char -> Parser Char
caseInsensitiveChar c = char (toLower c) <|> char (toUpper c)
-- Match the string 's', accepting either lowercase or uppercase form of each character 
caseInsensitiveString :: String -> Parser String
caseInsensitiveString s = try (mapM caseInsensitiveChar s) Text.Parsec.<?> "\"" ++ s ++ "\""

-- ORDER BY
singleOrderingParser :: Parser (String, Bool)
singleOrderingParser = do
    spaces
    column <- identifierParser
    spaces
    order <- caseInsensitiveString "ASC" <|> caseInsensitiveString "DESC"
    if capitalize order == "ASC" then
        return $ (column, True)
    else
        return $ (column, False)
    where capitalize = map (\x -> toUpper x)

listOrderingParser :: Parser [(String, Bool)]
listOrderingParser = sepBy1 singleOrderingParser (char ',')

orderByParser :: Parser [(String, Bool)]
orderByParser = do
    spaces
    caseInsensitiveString "ORDER"
    spaces
    caseInsensitiveString "BY"
    spaces
    orders <- listOrderingParser
    return orders