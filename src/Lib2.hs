{-# OPTIONS_GHC -Wno-unused-top-binds #-}

module Lib2
  ( parseStatement,
    executeStatement,
    ParsedStatement
  )
where

-- Original imports
import DataFrame (DataFrame)
import InMemoryTables (TableName)

-- Imports 
import DataFrame (DataFrame (DataFrame), Column (Column), ColumnType (IntegerType, BoolType, StringType), Value (IntegerValue), Row)
import InMemoryTables (TableName, database)
import DataFrame
import InMemoryTables
import Lib1
import Data.Maybe (isNothing, fromJust)
import Data.Either (fromRight, fromLeft)
import Data.Char


type ErrorMessage = String
type Database = [(TableName, DataFrame)]

-- Keep the type, modify constructors
data ParsedStatement
  = ColumnList [String] TableName -- Columns TableName
  | MinAggregation String TableName -- Column TableName
  | SumAggregation String TableName -- Column TableName
  | WhereOrCondition [String] [IntCondition] TableName -- Columns Conditions TableName

  | ShowTables
  | ShowTableName TableName
  deriving (Show, Eq)

type IntCondition = String


-- Parses user input into an entity representing a parsed
-- statement
parseStatement :: String -> Either ErrorMessage ParsedStatement
parseStatement input =
  let
    -- Remove everything after the semicolon and split everything by 'SPACE' in words
    (statement, semicolon) = break (==';') input
    words = splitBySpace statement
  in
  -- Check if statement is ended by ';'
  if (null semicolon) then Left "Missing statement end symbol: ';'."
  else
    -- SHOW TABLES
    if (length words) == 2 && (capitalize(words!!0 ++ words!!1)) == "SHOWTABLES"
      then Right ShowTables
    -- SHOW TABLE "name"
    else if (length words) == 3 && (capitalize (words!!0 ++ words!!1)) == "SHOWTABLE"
      then Right (ShowTableName (words!!2))
    -- SELECT
    else if (length words) >= 3 && capitalize (words!!0) == "SELECT"
      then
        let
          noSelectWord = tail words -- remove 'SELECT' keyword
          (select, _from) = break (\ x -> (capitalize x) == "FROM") noSelectWord -- separate 'SELECT' and 'FROM'
        in
        if (null _from) || (length _from) == 1 then Left "Incorrect SELECT statement format."
        else
        let
          from' = tail _from -- remove 'FROM' keyword
          (from, _wher) = break (\ x -> (capitalize x) == "WHERE") from' -- separate 'FROM' and 'WHERE'
          tName = head from -- take first occuring TableName
        in
        -- multiple 'FROM's 
        if (null _wher && length from > 1)
          then Left "Selecting from multiple tables is not allowed."
        -- if no 'WHERE' keyword
        else if (null _wher)
          then
            -- MIN
            if length select == 1 && isAggregateFunc (select!!0) "MIN"
              then Right $ MinAggregation (extractArgumentFromAggregateFunc $ select!!0) tName
            else
            -- SUM
            if length select == 1 && isAggregateFunc (select!!0) "SUM"
              then Right $ SumAggregation (extractArgumentFromAggregateFunc $ select!!0) tName
            -- COLUMN LIST
            else Right (ColumnList select tName)
        -- if WHERE 'keyword'
        else if (length _wher <= 1)
          then Left "Missing argumnets in the 'WHERE' clause."
        else if (length _wher > 1) -- For readability
          then
            let
              wher_ors = tail _wher -- conditions with ORs
              wher = removeORs wher_ors -- list of conditions
            in
            -- WHERE OR CONDITION
            if (null wher || last wher == "OR") then Left "Error in 'WHERE' clause."
            else Right $ WhereOrCondition (select) (wher) (tName)

        else Left "Not reachable"
    else
      Left "Not implemented: parseStatement"


removeORs :: [String] -> [String]
removeORs [] = []
removeORs input
  | length input >= 1 && capitalize (head input) == "OR" = []
  | length input >= 1 && capitalize (last input) == "OR" = []
  | length input >= 2 = 
    let
      (x : xs) = input
      (y : ys) = xs
    in
    if (capitalize y == "OR")
      then
        x : removeORs ys
    else if (capitalize x /= "OR" && capitalize y /= "OR")
      then ["OR"]
    else  ["OR"]
  | otherwise = input





isAggregateFunc :: String -> String -> Bool
isAggregateFunc input functionName = capitalize (take 4 input) == capitalize (functionName++"(") && last input == ')'

extractArgumentFromAggregateFunc :: String -> String
extractArgumentFromAggregateFunc input =
  let
    (a, b) = break (== ')') input
    (_, a1) = break (== '(') a
    a2 = tail a1
  in
    if b == ")" then a2;
    else ""

capitalize :: String -> String
capitalize = map toUpper

splitBySpace :: String -> [String]
splitBySpace [] = []
splitBySpace input =
  let
    (word, rest) = break (== ' ') input
    rest_ = dropWhile (== ' ') rest
  in
    word : splitBySpace rest_




-- Executes a parsed statemet. Produces a DataFrame. Uses
-- InMemoryTables.databases a source of data.
executeStatement :: ParsedStatement -> Either ErrorMessage DataFrame
executeStatement (ShowTables) = Right showTables
executeStatement (ShowTableName tName) = showTableByName tName

executeStatement (ColumnList columns tName) = columnList columns tName
executeStatement (MinAggregation argument tName) = Left "Execution of MIN() not yet implemented."
executeStatement (SumAggregation argument tName) = Left "Execution of SUM() not yet implemented."
executeStatement (WhereOrCondition columns conditions tName) = Left "Execution of WHERE OR not yet implemented"
executeStatement _ = Left "Not implemented: executeStatement"





-- SHOW TABLES (Lists avaivable tables in database)
showTables :: DataFrame
showTables = DataFrame (map (\x -> Column x StringType) (showTables_ database)) []
  where
    showTables_ :: Database -> [String]
    showTables_ [] = []
    showTables_ ((tName, dFrame) : xs) =
      tName : showTables_ xs


-- SHOW TABLE 'name' (Lists columns avaivable in the table 'name')
showTableByName :: String -> Either ErrorMessage DataFrame
showTableByName tName = showTableByName_ database tName
  where
    showTableByName_ :: Database -> String -> Either ErrorMessage DataFrame
    showTableByName_ db tableName =
      let
        maybeDFrame = getTable db tableName
      in
        if (maybeDFrame == Nothing) then Left ("Table with name: '" ++ tableName ++ "' does not exist.")
        else
          let (DataFrame columns rows) = fromJust maybeDFrame in
          Right (DataFrame columns [])






-- COLUMN LIST
--
-- Selects given [Column] from a given Table
columnList :: [String] -> TableName -> Either ErrorMessage DataFrame
columnList columnNames tName =
  let dFrame = getTable database tName in
  if (dFrame == Nothing) then Left ("Table with name: '" ++ tName ++ "' does not exist.")
  else
    let dFrame_ = fromJust dFrame in
    columnListDF columnNames dFrame_

-- Selects given [Column] from the given DataFrame
columnListDF :: [String] -> DataFrame -> Either ErrorMessage DataFrame
columnListDF columnNames dFrame =
    let cols = getColsFromDataFrame columnNames dFrame in
    if(cols == Nothing) then Left ("Some column names are incorrect.")
    else
      let cols_ = fromJust cols in
      Right (mergeListOfDataFrames cols_)

-- Returns a list of DataFrames that each represent each column of the given DataFrame
getColsFromDataFrame :: [String] -> DataFrame -> Maybe [DataFrame]
getColsFromDataFrame [] _ = Just []
getColsFromDataFrame (x : xs) dFrame = do
  current <- getColumnFromTable x dFrame
  rest <- getColsFromDataFrame xs dFrame
  return (current : rest)

-- Finds and returns the Column in the given DataFrame by name
getColumnFromTable :: String -> DataFrame -> Maybe DataFrame
getColumnFromTable columnName dFrame =
  if(columnName == "*") then Just dFrame
    else
    let
      cols = getColumnByName columnName dFrame
    in
      if (null cols) then Nothing
      else
        let
          col = head cols
          colValues = getColValues col dFrame
          colValues2dArray = arrayTo2D colValues
        in
          Just (DataFrame cols colValues2dArray)
  where
    arrayTo2D arr = map (\a -> [a]) arr

-- Merges the given DataFrames
mergeDataFrames :: DataFrame -> DataFrame -> DataFrame
mergeDataFrames dFrame_1 dFrame_2 =
  let
    (DataFrame cols1 rows1) = dFrame_1
    (DataFrame cols2 rows2) = dFrame_2
  in
  DataFrame (cols1 ++ cols2) (mergeRows rows1 rows2)
    where
      mergeRows :: [Row] -> [Row] -> [Row]
      mergeRows a [] = a
      mergeRows (x : xs) (y : ys) =
       (x ++ y) : (mergeRows xs ys)

-- Merges a list of given DataFrames
mergeListOfDataFrames :: [DataFrame] -> DataFrame
mergeListOfDataFrames [] = DataFrame [] []
mergeListOfDataFrames (x : xs) =
  if null xs then x
  else
    mergeDataFrames x (mergeListOfDataFrames xs)


-- Returns a single Column(in a list) that matches the given String in the DataFrame
getColumnByName :: String -> DataFrame -> [Column]
getColumnByName colName (DataFrame cols _) =
  findIt colName cols
  where
    findIt :: String -> [Column] -> [Column]
    findIt _ [] = []
    findIt name ((Column cName t) : xs) =
      if name == cName then  [(Column cName t)]
      else findIt name xs

-- Returns the list of [Value] in the provided Column
getColValues :: Column -> DataFrame -> [Value]
getColValues colName (DataFrame cols rows) =
  case getIndex colName cols 0 of
    Nothing -> []
    a ->
      let index = ((\(Just x)-> x)  a) in
      getNthElements index rows
  where
    getNthElements :: Int -> [[a]] -> [a]
    getNthElements index lists = map (\list -> list !! index) lists

getIndex :: Eq a => a -> [a] -> Int -> Maybe Int
getIndex _ [] _ = Nothing
getIndex item (x : xs) index =
  if item == x then Just index
  else getIndex item xs (index + 1)




-- Returns a Maybe DataFrame from the given database by name (Case sensitive)
getTable :: Database -> String -> Maybe DataFrame
getTable [] _ = Nothing
getTable _ [] = Nothing
getTable ((name, dataframe): xs) tName =
  if name == tName then Just dataframe else getTable xs tName












-- TEST DATA
column1 = Column "Column1" IntegerType
column2 = Column "Column2" StringType
column3 = Column "C" BoolType
row1 = [IntegerValue 5, StringValue "ddddddddddd", BoolValue True]
row2 :: [Value]
row2 = [IntegerValue 10, StringValue "no", BoolValue False]
testData :: DataFrame
testData = DataFrame [column1, column2, column3] [row1, row2]