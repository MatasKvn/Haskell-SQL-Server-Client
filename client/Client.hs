{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE OverloadedStrings #-}
{-# OPTIONS_GHC -Wno-incomplete-patterns #-}

module Main where
    
import Network.Wreq
import Control.Lens hiding ((.=))
import Data.Yaml hiding (Value)
--import qualified Data.ByteString.Lazy.Char8 as BL
import qualified Data.ByteString.Char8 as BS
import Data.ByteString.Lazy.Char8 as LBS ( unpack, pack)
import Data.Maybe (fromJust)
import Control.Monad (mzero)
import DataFrame (Column (..), ColumnType (..), Value (..), Row, DataFrame (..))
import ParsingParsec
import Lib1 (renderDataFrameAsTable)
import Control.Applicative ((<|>))




instance ToJSON ParsedStatement where
    toJSON (SelectStatement columns name conditions order) = object ["columns" .= columns, "SelectStatement" .= name, "conditions" .= conditions, "order" .= order]
    toJSON (DeleteStatement name conditions) = object ["DeleteStatement" .= name, "conditions" .= conditions]
    toJSON (UpdateStatement name values conditions) = object ["UpdateStatement" .= name, "values" .= values, "conditions" .= conditions]
    toJSON (InsertStatement name columns values) = object ["InsertStatement" .= name, "columns" .= columns, "values" .= values]
    toJSON (CreateTable name columns) = object ["CreateTable" .= name, "columns" .= columns]
    toJSON (DropTable name) = object ["DropTable" .= name]


instance ToJSON ColumnType where
    toJSON (IntegerType) = "IntegerType"
    toJSON (StringType) = "StringType"
    toJSON (BoolType) = "BoolType"


main :: IO ()
main = do
  putStrLn "Enter SQL query (or type 'quit' to exit):"
  query <- getLine
  if query == "quit"
    then putStrLn "Goodbye!"
    else do
      case parseSql query of
        Left err -> putStrLn $ "Error: " ++ err
        Right parsedStatement -> do
          case parsedStatement of
            (SelectStatement _ _ _ _) -> do
              let yamlStatement = encode parsedStatement
              response <- post "http://localhost:8080" yamlStatement
              let yamlResponse = response ^. responseBody
              case decodeEither' (BS.toStrict yamlResponse) :: Either ParseException DataFrame of
                Left err -> putStrLn $ "Error: " ++ (prettyPrintParseException err)
                Right dataframe -> putStrLn $ renderDataFrameAsTable 1000 dataframe
            other -> do
              let yamlStatement = encode parsedStatement
              response <- post "http://localhost:8080" yamlStatement
              let sqlResponse = response ^. responseBody
              putStrLn $ LBS.unpack sqlResponse
      main

instance FromJSON ColumnType where
  parseJSON (String "IntegerType") = return IntegerType
  parseJSON (String "StringType")  = return StringType
  parseJSON (String "BoolType")    = return BoolType
  parseJSON _                   = fail "Invalid ColumnType"

instance FromJSON Column where
  parseJSON (Object v) = 
    (Column <$> v .: "IntegerType" <*> pure IntegerType)
    <|> (Column <$> v .: "StringType" <*> pure StringType)
    <|> (Column <$> v .: "BoolType" <*> pure BoolType)
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


-- main :: IO ()
-- main = putStrLn "Ran CLIENT"



-- main :: IO ()
-- main = do
--   putStrLn "Enter SQL query:"
--   query <- getLine
--   case parseSql query of
--     Left err -> putStrLn $ "Error: " ++ err
--     Right parsedStatement -> do
--       let yamlStatement = encode (toJSON parsedStatement)
--       response <- post "http:" yamlStatement
--       let yamlResponse = response ^. responseBody
--       case decode yamlResponse of
--         Nothing -> putStrLn "Error: Could not decode server response"
--         Just dataframe -> putStrLn $ renderDataFrameAsTable 100 dataframe

