{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE OverloadedStrings #-}
{-# OPTIONS_GHC -Wno-incomplete-patterns #-}

import Network.Wreq
import Control.Lens
import Data.Yaml
--import qualified Data.ByteString.Lazy.Char8 as BL
import qualified Data.ByteString.Char8 as BS
import Data.Maybe (fromJust)
import Control.Monad (mzero)
import DataFrame (Column (..), ColumnType (..), Value (..), Row, DataFrame (..))
import ParsingParsec
import Lib1 (renderDataFrameAsTable)




instance ToJSON ParsedStatement where
    toJSON (SelectStatement columns name conditions order) = object ["columns" .= columns, "SelectStatement" .= name, "conditions" .= conditions, "order" .= order]
    toJSON (DeleteStatement name conditions) = object ["DeleteStatement" .= name, "conditions" .= conditions]
    toJSON (UpdateStatement name values conditions) = object ["UpdateStatement" .= name, "values" .= values, "conditions" .= conditions]
    toJSON (InsertStatement name columns values) = object ["InsertStatement" .= name, "columns" .= columns, "values" .= values]
    toJSON (CreateTable name columns) = object ["CreateTable" .= name, "columns" .= columns]
    toJSON (DropTable name) = object ["DropTable" .= name]



module Main where

-- main :: IO ()
-- main = putStrLn "Ran CLIENT"



main :: IO ()
main = do
  putStrLn "Enter SQL query:"
  query <- getLine
  case parseSql query of
    Left err -> putStrLn $ "Error: " ++ err
    Right parsedStatement -> do
      let yamlStatement = encode (toJSON parsedStatement)
      response <- post "http:" yamlStatement
      let yamlResponse = response ^. responseBody
      case decode yamlResponse of
        Nothing -> putStrLn "Error: Could not decode server response"
        Just dataframe -> putStrLn $ renderDataFrameAsTable 100 dataframe

