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
import DataFrame (DataFrame (DataFrame), Column (Column), ColumnType (IntegerType, BoolType), Value (IntegerValue), Row)
import InMemoryTables (TableName, database)
import DataFrame
import InMemoryTables
import Lib1
import Data.Maybe (isNothing, fromJust)
import Data.Either (fromRight, fromLeft)


type ErrorMessage = String
type Database = [(TableName, DataFrame)]

-- Keep the type, modify constructors
data ParsedStatement
  = ColumnList [String] TableName -- 
  | MinAggregation String TableName -- 
  | SumAggregation String TableName --
  | WhereOrCondition [Condition] TableName --

  | ShowTables 
  | ShowTableName TableName
  deriving (Show, Eq)

data Condition
  = Equals Int Int
  | NotEqual Int Int
  | LessThan Int Int
  | GreaterThan Int Int
  | LessThanOrEqual Int Int
  | GreaterThanOrEqual Int Int
  deriving (Show, Eq)



-- Parses user input into an entity representing a parsed
-- statement
parseStatement :: String -> Either ErrorMessage ParsedStatement
parseStatement input =
  let
    words = splitBySpace input
  in 
    Left "Not implemented: parseStatement"


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
executeStatement _ = Left "Not implemented: executeStatement"





-- SHOW TABLES (Lists avaivable tables in database)
showTables :: [String]
showTables = showTables_ database
  where
    showTables_ :: Database -> [String]
    showTables_ [] = []
    showTables_ ((tName, dFrame) : xs) =
      tName : showTables_ xs 
  

-- SHOW TABLE 'name' (Lists columns avaivable in the table 'name')
showTableByName :: String -> Either ErrorMessage [String]
showTableByName tName = showTableByName_ database tName
  where
    showTableByName_ :: Database -> String -> Either ErrorMessage [String]
    showTableByName_ db tableName =
      let 
        maybeDFrame = getTable db tableName
      in
        if (maybeDFrame == Nothing) then Left ("Table with name: '" ++ tableName ++ "' does not exist.")
        else 
          let (DataFrame columns rows) = fromJust maybeDFrame in
          Right (getColumnsNames columns)
    getColumnsNames :: [Column] -> [String]
    getColumnsNames [] = []
    getColumnsNames ((Column cName cType) : xs) =
      cName : getColumnsNames xs  






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

-- Doesn't work bad data( NO ERROR HANDLING )
getColumnFromTable :: String -> DataFrame -> Maybe DataFrame
getColumnFromTable columnName dFrame =
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





-- Case sensitive getTable
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