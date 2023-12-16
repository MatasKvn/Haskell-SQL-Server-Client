{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE OverloadedStrings #-}
{-# OPTIONS_GHC -Wno-incomplete-patterns #-}
{-# OPTIONS_GHC -Wno-unrecognised-pragmas #-}
{-# HLINT ignore "Use camelCase" #-}

module Main where

import Control.Concurrent
import Control.Monad
import DataFrame
import Lib1 (renderDataFrameAsTable)
import Web.Scotty
import Data.Text.Lazy.Encoding (decodeUtf8)
import ParsingParsec
import Data.ByteString.Lazy.Char8 as LBS ( unpack, pack)
import Data.Text.Lazy as TL (pack)



main :: IO ()
main = scotty 8080 $ do
    post "/" $ do
        body <- body
        let idk = ParsingParsec.parseSql (LBS.unpack body)
        case idk of
            Left err -> text $ "An error occured.\n" <> TL.pack err
            Right parsedStatement -> text $ "Statement parsed: " <> TL.pack (show parsedStatement)
        
-- run server & run
-- curl -X POST -d "select * from a;" http://localhost:8080
-- for calling this post method