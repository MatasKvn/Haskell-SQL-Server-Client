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
import DataFrame
import Lib1 (renderDataFrameAsTable)
import Web.Scotty
import Data.Text.Lazy.Encoding (decodeUtf8)
import ParsingParsec
import Data.ByteString.Lazy.Char8 as LBS ( unpack, pack)
import qualified Data.ByteString.Char8 as BS
import Data.Text.Lazy as TL (pack)
import Control.Applicative ((<|>))
import Control.Monad.IO.Class (liftIO)


main :: IO ()
main = scotty 8080 $ do
    post "/" $ do
        body <- body
        let idk = decodeEither' (BS.toStrict body) :: Either ParseException ParsedStatement
        case idk of
            Left err -> text $ "An error occured.\n" <> TL.pack (prettyPrintParseException err)
            Right parsedStatement -> text $ "Statement parsed: " <> TL.pack (show parsedStatement)

-- run server & run
-- curl -X POST -d "select * from a;" http://localhost:8080
-- for calling this post method


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

instance FromJSON ColumnType where
    parseJSON (String "IntegerType") = return IntegerType
    parseJSON (String "StringType")  = return StringType
    parseJSON (String "BoolType")    = return BoolType
    parseJSON _                   = fail "Invalid ColumnType"

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