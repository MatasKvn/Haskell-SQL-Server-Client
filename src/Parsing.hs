{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE OverloadedStrings #-}
{-# OPTIONS_GHC -Wno-incomplete-patterns #-}

module Parsing () where

import Control.Monad.State
import Control.Monad.Trans.Either
import Control.Applicative ((<|>), Alternative (empty))
import Data.Char (toLower, toUpper)

type ParseError = String
type Parser a = EitherT ParseError (State String) a

throwE :: Monad m => e -> EitherT e m a 
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
  -- a <- parseSeparated1 (parseString "ab") (parseString' "OR")
  a <- parseWhileCan1 $ (parseString' "a")
  -- a <- parseWhileCan0 $ combinedParser (parseString "ab") (parseString ";")
  return a



sqlParser :: String -> Either ErrorMessage ParsedStatement
sqlParser statement =
  case runParser parseSql statement of
    Left err -> Left err
    Right result -> Right result


parseSql :: Parser ParsedStatement
parseSql = do 
  a <- parseSelect <|> parseDelete
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

  -- ;
  parseString ";"
  return $ SelectStatement columns tables []

parseDelete :: Parser ParsedStatement
parseDelete = do
  -- DELETE
  parseSpaces
  parseString' "DELETE"
  parseSpaces
  parseString' "FROM"
  table <- parseTableName
  return $ DeleteStatement table [] 






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