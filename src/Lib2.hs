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

-- Imports whole modules for usage in GHCI
import DataFrame (DataFrame (DataFrame), Column (Column), ColumnType (IntegerType, BoolType), Value (IntegerValue))
import InMemoryTables (TableName)
import DataFrame
import InMemoryTables
import Lib1


type ErrorMessage = String
type Database = [(TableName, DataFrame)]

-- Keep the type, modify constructors
data ParsedStatement
  = ColumnList [String] TableName -- 
  | MinAggregation String TableName -- 
  | SumAggregation String TableName --
  | WhereOrCondition [Condition] TableName --
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
parseStatement _ = Left "Not implemented: parseStatement"

-- Executes a parsed statemet. Produces a DataFrame. Uses
-- InMemoryTables.databases a source of data.
executeStatement :: ParsedStatement -> Either ErrorMessage DataFrame
executeStatement _ = Left "Not implemented: executeStatement"





-- SHOW TABLES (Lists avaivable tables in database)
showTables :: Database -> [String]
showTables [] = []
showTables ((tName, dFrame) : xs) =
  tName : showTables xs 
  
-- SHOW TABLE 'name' (Lists columns avaivable in the table 'name')
showTableByName :: Database -> String -> Maybe [Column]
showTableByName db tableName =
  let 
    maybeDFrame = getTable db tableName
  in
    if(maybeDFrame == Nothing) then Nothing
    else 
      let (DataFrame columns rows) = (\(Just x)-> x)  maybeDFrame in
        Just columns

-- Doesn't work bad data( NO ERROR HANDLING )
getColumnFromTable :: String -> DataFrame -> DataFrame
getColumnFromTable columnName dFrame =
  let
    cols = getColumnByName columnName dFrame
    col = head cols
    colValues = getColValues col dFrame
    colValues2dArray = arrayTo2D colValues
  in
    DataFrame cols colValues2dArray
  where
    arrayTo2D arr = map (\a -> [a]) arr





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