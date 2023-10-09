{-# OPTIONS_GHC -Wno-unused-top-binds #-}
{-# OPTIONS_GHC -Wno-unrecognised-pragmas #-}
{-# HLINT ignore "Use foldr" #-}
{-# HLINT ignore "Redundant if" #-}
{-# HLINT ignore "Redundant ==" #-}
{-# HLINT ignore "Use if" #-}
{-# OPTIONS_GHC -Wno-incomplete-patterns #-}

module Lib1
  ( parseSelectAllStatement,
    findTableByName,
    validateDataFrame,
    renderDataFrameAsTable,
  )
where

import DataFrame (DataFrame (..), Row, Column (..), ColumnType (..), Value (..))
import InMemoryTables (TableName, database)
-- import GHC.Windows (getErrorMessage)
import Control.Arrow (ArrowChoice(left))
import GHC.Conc (par)
import GHC.Unicode
import Prelude


type ErrorMessage = String

type Database = [(TableName, DataFrame)]

-- Your code modifications go below this comment

-- 1) implement the function which returns a data frame by its name
-- in provided Database list
findTableByName :: Database -> String -> Maybe DataFrame
findTableByName [] _ = Nothing
findTableByName _ [] = Nothing
findTableByName ((table, dataframe):xs) tableName =
    if stringIsEqual table tableName then Just dataframe else findTableByName xs tableName

stringIsEqual :: String -> String -> Bool
stringIsEqual (x:xs) (y:ys) = if x == y || toUpper x == y || toLower x == y then stringIsEqual xs ys else False
stringIsEqual [] _ = True
stringIsEqual _ [] = False

-- 2) implement the function which parses a "select * from ..."
-- sql statement and extracts a table name from the statement
parseSelectAllStatement :: String -> Either ErrorMessage TableName
parseSelectAllStatement [] = Left "Empty input"
parseSelectAllStatement input =
  case splitBySpace input of
    [a, b, c, d] ->
      if parseSelect a == True && b == "*" && parseFrom c == True && last d == ';'
        then
          let (tableName, _) = break (== ';') d in
          Right tableName
      else Left "Statement is incorrect"
    _ -> Left "Format is incorrect"

splitBySpace :: String -> [String]
splitBySpace [] = []
splitBySpace str =
  let (word, rest) = break isSpace str
      rest' = dropWhile isSpace rest  -- Remove leading spaces in 'rest'
  in if null word
       then splitBySpace rest'  -- Skip empty word
       else word : splitBySpace rest'
  where
    isSpace c = c == ' '

-- Parse differend words
parseSelect :: String -> Bool
parseSelect [] = False
parseSelect a =
  if stringToLower a == "select"
    then True
  else False

parseFrom :: String -> Bool
parseFrom [] = False
parseFrom a =
  if stringToLower a == "from"
    then True
  else False

stringToLower :: String -> String
stringToLower [] = ""
stringToLower (x:xs) = toLower x : stringToLower xs

-- -- Unused function
-- dbContainsTableName :: Database -> String -> Bool
-- dbContainsTableName _ [] = False
-- dbContainsTableName [] _ = False
-- dbContainsTableName ((tableName, _):xs) str = 
--   if tableName == str 
--     then True
--   else dbContainsTableName xs str


-- 3) implement the function which validates tables: checks if
-- columns match value types, if rows sizes match columns,..

validateDataFrame :: DataFrame -> Either ErrorMessage ()
validateDataFrame (DataFrame columns rows) =
    if sizeCheck columns rows
        then
            if typeCheck columns rows
            then Right ()
            else Left "Row values don't match column value types"
        else Left "Row sizes don't match columns"
    where
        sizeCheck :: [Column] -> [Row] -> Bool
        sizeCheck _ [] = True
        sizeCheck column (x:xs)= (length x == length column) && sizeCheck column xs

        typeCheck :: [Column] -> [Row] -> Bool
        typeCheck _ [] = True
        typeCheck column (x:xs) = typeMatch column x && typeCheck column xs
            where
                typeMatch ::[Column] -> [Value] -> Bool
                typeMatch [] [] = True
                typeMatch _ [] = False
                typeMatch [] _ = False
                typeMatch (c:cs) (v:vs) = typeMatchInner c v && typeMatch cs vs
                    where
                        typeMatchInner :: Column -> Value -> Bool
                        typeMatchInner (Column _ col) val = getType col == getValue val || getValue val == "Null"
                        
getType :: ColumnType -> String
getType IntegerType = "Integer"
getType StringType  = "String"
getType BoolType    = "Bool"

getValue :: Value -> String
getValue (IntegerValue _ )  = "Integer"
getValue (StringValue _ )   = "String"
getValue (BoolValue _ )     = "Bool"
getValue  NullValue         = "Null"

-- 4) implement the function which renders a given data frame
-- as ascii-art table (use your imagination, there is no "correct"
-- answer for this task!), it should respect terminal
-- width (in chars, provided as the first argument)
renderDataFrameAsTable :: Integer -> DataFrame -> String 
renderDataFrameAsTable width (DataFrame collumns rows) = 
  let 
    lengths = getListOfLongestValues (DataFrame collumns rows) 
  in
  -- if(isWidthExceeded width lengths) then "Provided width is not enough for the table" else -- Stops tables that are too big from being printed
  concatenateRows rows (columnsNamesToString collumns lengths) lengths
    where 
      isWidthExceeded :: Integer -> [Integer] -> Bool
      isWidthExceeded width colWidths = if(width < sumTableWidth colWidths) then True else False
         where 
           sumTableWidth :: [Integer] -> Integer
           sumTableWidth [] = 1 -- 1 for the last '|'
           sumTableWidth (x : xs) =  x + 1 + sumTableWidth xs -- +1 for '|' separators

rowToString :: Row -> [Integer] -> String
rowToString [] _ = "|"
rowToString (x : xs) (y:ys) = "|"++ extractVarFromValue x ++ addSpaces(y - toInteger (extractLengthFromValue x)) ++ rowToString xs ys

addSpaces :: Integer -> String
addSpaces x = if x > 0 then ' ' : addSpaces (decrement x) else ""

decrement :: Integer -> Integer
decrement x = x - 1

extractVarFromValue :: Value -> String
extractVarFromValue (StringValue s) = s
extractVarFromValue (IntegerValue i) = show i
extractVarFromValue (BoolValue b) = show b
extractVarFromValue (NullValue) = "null"

concatenateRows :: [Row] -> String -> [Integer] -> String
concatenateRows [] res _ = res
concatenateRows (x:xs) res lengths = concatenateRows xs (res ++ "\n" ++ rowToString x lengths) lengths

columnsNamesToString :: [Column] -> [Integer] -> String
columnsNamesToString [] _ = "|"
columnsNamesToString (x : xs) (y : ys) = "|" ++ columnToString x ++ addSpaces(y - toInteger (length (columnToString x))) ++ columnsNamesToString xs ys
 where columnToString (Column name _) = name

extractLengthFromValue :: Value -> Int
extractLengthFromValue (StringValue s) = length s
extractLengthFromValue (IntegerValue i) = length (show i)
extractLengthFromValue (BoolValue b) = length (show b)
extractLengthFromValue (NullValue) = length "Null"

getNthElements :: Int -> [[a]] -> [a]
getNthElements n lists = map (\list -> list !! n) lists


-- Monster function xddddddddddddd
getListOfLongestValues :: DataFrame -> [Integer]
getListOfLongestValues (DataFrame columns rows) = longestValue columns rows 0
  where
    longestValue :: [Column] -> [Row] -> Int -> [Integer]
    longestValue [] [] index = []
    longestValue columns rows index =
      if index >= length columns then [] 
      else
      -- indexMaxLength : (longestValue columns rows index+1)
      getMaxLengthInColumn columns rows index : longestValue columns rows (increment index)
        where 
          -- Finds max length in each column
          getMaxLengthInColumn :: [Column] -> [Row] -> Int -> Integer
          getMaxLengthInColumn columns rows index =
            isColOrRowsLonger (columns !! index) (maxValueLength (getNthElements index rows))
              where
                isColOrRowsLonger :: Column -> Integer -> Integer
                isColOrRowsLonger column x = max x (getColumnNameLength column)

increment :: Int -> Int
increment x = x + 1

getColumnNameLength :: Column -> Integer
getColumnNameLength (Column name _) = toInteger (length name)

maxValueLength :: [Value] -> Integer
maxValueLength values = maxV values 0
  where
  maxV :: [Value] -> Int -> Integer
  maxV [] currentMax = toInteger currentMax
  maxV (x : xs) currentMax =
      let currentVal = extractLengthFromValue x in
      if currentMax > currentVal
       then maxV xs currentMax
      else maxV xs currentVal
