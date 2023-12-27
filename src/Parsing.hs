{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE OverloadedStrings #-}
{-# OPTIONS_GHC -Wno-incomplete-patterns #-}

module Parsing () where

import Control.Monad.State
import Control.Monad.Trans.Either
import Control.Applicative ((<|>))
import Data.Char (toLower, toUpper)
import Control.Monad.Error (MonadError(catchError))

type ParseError = String
type Parser a = EitherT ParseError (State String) a

throwE :: Monad m => ParseError -> EitherT ParseError m a 
throwE err = EitherT $ return $ Left err


runParser :: Parser a -> String -> Either String a
runParser parser input = evalState (runEitherT parser) input


-- Parse given char
parseChar :: Char -> Parser Char
parseChar a = do
  inp <- lift get
  case inp of
    [] -> throwE "Empty input."
    (x : xs) ->
      if a == x
        then do
          lift $ put xs
          return a
        else throwE ([a] ++ " expected but " ++ [x] ++ " found.\n")

-- Parses the given char (case insensitive)
parseChar' :: Char -> Parser Char
parseChar' a = do
  parseChar (toLower a) <|> parseChar (toUpper a)

-- Parses the given string
parseString :: String -> Parser String
parseString str = do
  inp <- lift get
  let taken = take (length str) inp
  if length taken == length str && taken == str then do
    lift $ put (drop (length str) inp)
    return (str)
  else
    throwE ("Expected: '" ++ str ++ "' but found: '" ++ taken ++ "'")
      
-- Parses the given string (case insensitive)
parseString' :: String -> Parser String
parseString' str = do
  inp <- lift get
  let taken = take (length str) inp
  if length taken == length str && capitalize taken == capitalize str then do
    lift $ put (drop (length str) inp)
    return (str)
  else
    throwE ("Expected: '" ++ str ++ "' but found: '" ++ taken ++ "'")
    where capitalize a = map toUpper a

-- Drops 0 or more leading spaces
parseSpaces :: Parser ()
parseSpaces = do
  inp <- lift get
  lift $ put (dropWhile isSpace inp)
    where isSpace a = elem a [' ', '\n', '\0', '\t']

-- Parses String if parsable
parseStringOptional :: String -> Parser String
parseStringOptional str = do
  inp <- lift get
  let taken = take (length str) inp 
  if length taken == length str && taken == str then do
    lift $ put (drop (length str) inp)
    return str
  else
    return ""

-- Parses String (case insensitive) if parsable
parseStringOptional' :: String -> Parser String
parseStringOptional' str = do
  inp <- lift get
  let taken = take (length str) inp 
  if length taken == length str && capitalize taken == capitalize str then do
    lift $ put (drop (length str) inp)
    return str
  else
    return ""
      where capitalize a = map toUpper a


-- Parses 0 or more given string parsers
parseWhileCan0 :: Parser String -> Parser [String]
parseWhileCan0 parser = do
  p1 <- parser <|> parseString' ""
  case p1 of 
    [] -> return []
    p1 -> do
      rest <- parseWhileCan0 parser
      return (p1 : rest)
-- Parses 0 or more given string parsers
parseWhileCan :: Parser String -> Parser [String]
parseWhileCan parser = do
  p1 <- parser <|> parseString' ""
  case p1 of 
    [] -> return []
    p1 -> do
      rest <- parseWhileCan0 parser
      return (p1 : rest)

-- ab 


-- Parses 1 or more given string parsers
parseWhileCan1 :: Parser String -> Parser [String]
parseWhileCan1 parser = do
  required <- parser
  p1 <- parser <|> parseString' ""
  case p1 of 
    [] -> return []
    p1 -> do
      rest <- parseWhileCan0 parser
      return (required : p1 : rest)


-- Parser -> ParserSeparator -> res
-- Parse 1 or more separated by ParserSeparator
parseSeparated1 :: Parser String -> Parser String -> Parser [String]
parseSeparated1 parser1 parser2 = do
  p1 <- parser1
  rest <- parseWhileCan0 $ combinedParser parser1 parser2
  return $ p1 : rest

  where
    combinedParser :: Parser a -> Parser a -> Parser a
    combinedParser p1 p2 = do
      a <- p2
      b <- p1
      return b

combinedParser :: Parser a -> Parser a -> Parser a
combinedParser p1 p2 = do
  a <- p1
  b <- p2
  return a

parseTableName :: Parser String
parseTableName = do
  let allowedSymbols = ['0'..'9'] ++ ['a'..'z'] ++ ['A'..'Z'] ++ ['\'', '=', '>', '<', '/', '*']
  parseSpaces
  identifier <- parseStringOfGivenSymbols allowedSymbols
  parseSpaces
  return identifier

parseStringOfGivenSymbols :: [Char] -> Parser String
parseStringOfGivenSymbols chars = do
  inp <- lift get
  case inp of
    [] -> throwE $ "Expected one of: " ++ chars ++ " but got: []"
    (x : xs) -> 
      if x `elem` chars then
        parseStringOfGivenSymbols' chars
      else
        throwE $ "Expected one of: " ++ chars ++ " but got: '" ++ [x] ++"'."
  where 
  parseStringOfGivenSymbols' :: [Char] -> Parser String
  parseStringOfGivenSymbols' chars = do
    inp <- lift get
    case inp of
      [] -> return $ []
      (x : xs) -> 
        if x `elem` chars then do
          put xs
          rest <- parseStringOfGivenSymbols' chars
          return $ x : rest
        else
          return $ []
  


fparser :: Parser [String]
fparser = do
  p1 <- parseTableName
  rest <- parseWhileCan0 $ combinedParsera (parseTableName) (parseString' "OR ")
  return $ p1 : rest

  where
combinedParsera :: Parser a -> Parser a -> Parser a
combinedParsera p1 p2 = do
  a <- p2
  b <- p1
  return b



sqlParser :: String -> Either ErrorMessage ParsedStatement
sqlParser statement =
  case runParser parseSql statement of
    Left err -> Left err
    Right result -> Right result


parseSql :: Parser ParsedStatement
parseSql = do 
  a <- parseSelect <|> parseDelete <|> parseUpdate -- <|> parseInsert 
  return a


parseSelect :: Parser ParsedStatement
parseSelect = do
  -- SELECT
  parseSpaces
  parseString' "SELECT"
  parseSpaces
  columns <- parseSeparated1 (parseTableName) (parseString' ",")
  -- FROM
  parseSpaces
  parseString' "FROM"
  parseSpaces
  tables <- parseSeparated1 (parseTableName) (parseString' ",")
  parseSpaces
  -- WHERE
  conditions <- parseWhereConditions
  parseSpaces
  -- ORDER BY
  orderings <- parseOrderBy
  parseSpaces
  parseString ";"

  return $ SelectStatement columns tables conditions orderings


parseWhereConditions :: Parser [String]
parseWhereConditions = do
  parseSpaces
  wher <- parseStringOptional' "WHERE"
  case wher of
    [] -> do
      return []
    str -> do
      conditions <- parseSeparated1 (parseTableName) (parseString' "OR ")
      return conditions

parseOrderBy :: Parser [(String, Bool)]
parseOrderBy = do
  order <- parseStringOptional' "ORDER"
  parseSpaces
  by <- parseStringOptional' "BY"
  
  if null order then
    return []
  else if null by then
    throwE $ "Expected BY"
  else 
    parseOrderings


parseOrdering :: Parser (String, Bool)
parseOrdering = do
  parseSpaces
  tName <- parseTableName `catchError` handleParseError
  parseSpaces
  order <- parseString' "ASC" <|> parseString' "DESC"
  parseSpaces
  case order of 
    "ASC" -> return (tName, True)
    "DESC" -> return (tName, False)
  where
    handleParseError :: String -> Parser String
    handleParseError err = return $ ""

parseOrderings :: Parser [(String, Bool)]
parseOrderings = do
  ord <- parseOrdering
  next <- parseStringOptional' ","
  case next of 
    [] -> return [ord]
    a -> do 
      rest <- parseOrderings 
      return $ ord : rest

-- a , b , c
  
parseTest :: Parser String
parseTest = do
  tName <- parseTableName `catchError` handleParseError
  return tName
  where
    handleParseError :: String -> Parser String
    handleParseError err = throwE "Error"



parseDelete :: Parser ParsedStatement
parseDelete = do
  -- DELETE
  parseSpaces
  parseString' "DELETE"
  parseSpaces
  parseString' "FROM"
  table <- parseTableName

  conditions <- parseWhereConditions

  parseString ";"
  return $ DeleteStatement table conditions 

parseUpdate :: Parser ParsedStatement
parseUpdate = do
  -- UPDATE
  parseSpaces
  parseString' "UPDATE"
  parseSpaces
  table <- parseTableName
  parseSpaces
  parseString' "SET"
  parseSpaces
  updates <- parseSeparated1 (parseTableName) (parseString' ",")
  parseSpaces
  conditions <- parseSeparated1 (parseTableName) (parseString' ",")


  return $ UpdateStatement table updates conditions

parseInsert :: Parser ParsedStatement
parseInsert = do
  -- INSERT

  return $ InsertStatement [] [] []




data ParsedStatement
  -- Condition = Condition(OR(int))
  = SelectStatement [String] [TableName] [String] [(String, Bool)]-- [Column] [Table] [Condition]
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