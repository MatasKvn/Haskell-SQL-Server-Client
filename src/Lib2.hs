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
import Data.List
import Text.Read
import Lib1 (renderDataFrameAsTable, findTableByName)

type ErrorMessage = String
type Database = [(TableName, DataFrame)]

-- Keep the type, modify constructors
data ParsedStatement
  = ColumnList [String] [String] TableName -- Columns Conditions TableName
  | MinAggregation String [String] TableName -- Argument Conditions TableName
  | SumAggregation String [String] TableName -- Argument Conditions TableName

  | ShowTables
  | ShowTableName TableName
  deriving (Show, Eq)


-- TODO:
-- MIN(), MAX()
-- add validations of columns tables in conditions so conditions like "a>1or" dont break the program

-- Parses user input into an entity representing a parsed
-- statement
parseStatement :: String -> Either ErrorMessage ParsedStatement
parseStatement input =
  let
    (beforeSemicolon, afterSemicolon) = break (== ';') input
    hasEnd = not (null afterSemicolon) && head afterSemicolon == ';'
    input'
      | not (null beforeSemicolon) && not (null afterSemicolon) && head afterSemicolon == ';' = beforeSemicolon
      | otherwise = []

    _select = splitString "SELECT" " FROM" input'
    _from = splitString " FROM" " WHERE" input'
    _wher = splitString " WHERE" "" input'

    select = splitByChar ',' (filter (/= ' ') (drop (length "SELECT ") _select))
    from = splitByChar ',' (filter (/= ' ') (drop (length " FROM ") _from))
    wher = map (filter (/= ' ')) (splitBySubstring " OR " (drop (length " WHERE ") _wher))
    showTableName = map (filter (/= ' '))(splitByChar ' '(splitString "SHOW TABLE " ";" input))
  in
  if not hasEnd then Left "Missing statement end symbol: ';'."
  else if capitalize (filter (/= ' ') input') == "SHOWTABLES" then Right ShowTables
  else if length showTableName == 3 && capitalize (showTableName!!0) == "SHOW" && capitalize (showTableName!!1) == "TABLE" then Right $ ShowTableName (showTableName!!2)
  else if null select then Left "Incorrect SELECT statement."
  else if null from || length from > 1 then Left "Incorrect FROM statement."
  else 
    Right $ ColumnList select wher (head from)
  
  -- MIN()
  -- if length select == 1 && isAggregateFunc (head select) "MIN" then Right $ MinAggregation (extractArgumentFromAggregateFunc (head select)) (head from)
  -- -- SUM()
  -- else if length select == 1 && isAggregateFunc (head select) "SUM" then Right $ SumAggregation (extractArgumentFromAggregateFunc (head select)) (head from)
  -- else 
  -- WHERE OR




executeSelect :: [String] -> [String] -> TableName -> Either ErrorMessage DataFrame
executeSelect columns conditions tName = do
  let
    maybDFrame = getTable database tName
  -- if (dFrame == Nothing) then Left ("Table with name: '" ++ tName ++ "' does not exist.")
  -- else
  case maybDFrame of 
    Nothing -> Left ("Table with name: '" ++ tName ++ "' does not exist.")
    Just dFrame -> do
      case filterDataframe dFrame conditions of
        Nothing -> Left ("Could not filter given table.")
        Just filteredDataFrame -> do
          case (columnListDF columns filteredDataFrame) of
            Left errorMessage -> Left ("Could not render table: '" ++ tName ++"'.")
            Right dFrame_ -> Right dFrame_


    
    
    

filterDataframe :: DataFrame -> [String] -> Maybe DataFrame
filterDataframe dFrame [] = Just dFrame
filterDataframe (DataFrame cols rows) conditions = do
  justRows <- (compareRows rows cols conditions)
  return (DataFrame cols justRows)

compareRows :: [Row] -> [Column] -> [String] -> Maybe [Row]
compareRows [] _ _ = Just []
compareRows (x : xs) columns conditions = do
  current <- compareRow x columns conditions
  rest <- compareRows xs columns conditions

  if current then return (x : rest)
  else return rest


compareRow :: Row -> [Column] -> [String] -> Maybe Bool
compareRow _ _ [] = Just False
compareRow row columns (x : xs) = do
  current <- compareSingle row columns x
  rest <- compareRow row columns xs

  return (current || rest)


compareSingle :: Row -> [Column] -> String -> Maybe Bool
compareSingle row columns condition = do

  compar <- extractComparison (getContainedComparison condition)
  (a1, a2) <- extractCondtionArguments condition

  let 
    arg1 = if null a1 then Just 0 else readMaybe a1 :: Maybe Integer
    arg2 = if null a2 then Just 0 else readMaybe a2 :: Maybe Integer

  case (arg1, arg2) of
    (Just i1, Just i2) -> return (compar i1 i2)
    (Nothing, Just i2) -> do
      index <- getIndex (head (getColumnByName a1 (DataFrame columns [row]))) columns 0
      let 
        value = row !! index
      i1 <- getFromValue value

      return (compar i1 i2)
    (Just i1, Nothing) -> do
      index <- getIndex (head (getColumnByName a2 (DataFrame columns [row]))) columns 0
      let 
        value = row !! index
      i2 <- getFromValue value

      return (compar i1 i2)
      
    (Nothing, Nothing) -> do
      index1 <- getIndex (head (getColumnByName a1 (DataFrame columns [row]))) columns 0
      index2 <- getIndex (head (getColumnByName a2 (DataFrame columns [row]))) columns 0
      let 
        value1 = row !! index1
        value2 = row !! index2
      i1 <- getFromValue value1
      i2 <- getFromValue value2

      return (compar i1 i2)
        
getFromValue :: Value -> Maybe Integer
getFromValue (IntegerValue a ) = Just a
getFromValue _ = Nothing



extractCondtionArguments :: String -> Maybe (String, String)
extractCondtionArguments [] = Nothing
extractCondtionArguments input =
  let
    comparisonStr = getContainedComparison input
    arg1 = removeStringFrom comparisonStr input
    arg2 = drop (length comparisonStr) (getStringFrom comparisonStr input)
  in
  if not (null $ getContainedComparison input) then Just (arg1, arg2)
  else Nothing

extractComparison :: String -> Maybe (Integer -> Integer -> Bool)
extractComparison input
  | checkWord "=" input = Just (==)
  | checkWord "!=" input = Just (/=)
  | checkWord "<=" input = Just (<=)
  | checkWord ">=" input = Just (>=)
  | checkWord "<" input = Just (<)
  | checkWord ">" input = Just (>)
extractComparison _ = Nothing

getContainedComparison :: String -> String
getContainedComparison [] = []
getContainedComparison input
  | checkWord "=" input = "="
  | checkWord "!=" input = "!="
  | checkWord "<=" input = "<="
  | checkWord ">=" input = ">="
  | checkWord "<" input = "<"
  | checkWord ">" input = ">"
  | otherwise = getContainedComparison (tail input)




splitBySubstring :: String -> String -> [String]
splitBySubstring [] a = [a]
splitBySubstring _ [] = []
splitBySubstring word input =
  splitByChar '\0' (replaceWordWithChar word '\0' input)

replaceWordWithChar :: String -> Char -> String -> String
replaceWordWithChar _ _ [] = []
replaceWordWithChar word char (x : xs) =
  if checkWord word (x : xs) then char : replaceWordWithChar word char (drop (length word) (x : xs))
  else x : replaceWordWithChar word char xs

-- Returns the String in between the first given and the second given Strings
splitString :: String -> String -> String -> String
splitString _ _ [] = []
splitString startWord endWord input =
  removeStringFrom endWord (getStringFrom startWord input)

-- Removes String after the first given String, doesn't remove anything if given String is empty
removeStringFrom :: String -> String -> String
removeStringFrom _ [] = []
removeStringFrom [] input = input
removeStringFrom word (x : xs) =
  if checkWord word (x : xs) then take (length word) []
  else x : removeStringFrom word xs

-- Returns a String that starts from the first given String
getStringFrom :: String -> String -> String
getStringFrom _ [] = []
getStringFrom [] input = input
getStringFrom word (x : xs) =
  if checkWord word (x : xs) then (x : xs)
  else getStringFrom word xs


checkWord :: String -> String -> Bool
checkWord word input =
  capitalize (take (length word) input) == capitalize word



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

splitByChar :: Char -> String -> [String]
splitByChar _ [] = []
splitByChar char input = splitByChar' char input
  where
    splitByChar' :: Char -> String -> [String]
    splitByChar' _ [] = []
    splitByChar' char input =
      let
        (word, rest) = break (== char) input
        rest_ = dropWhile (== char) rest
      in
        word : (splitByChar' char rest_)





-- Executes a parsed statemet. Produces a DataFrame. Uses
-- InMemoryTables.databases a source of data.
executeStatement :: ParsedStatement -> Either ErrorMessage DataFrame
executeStatement (ShowTables) = Right showTables
executeStatement (ShowTableName tName) = showTableByName tName

executeStatement (ColumnList columns conditions tName) = executeSelect columns conditions tName
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
a = Column "a" IntegerType
b = Column "b" IntegerType
c = Column "c" StringType
d = Column "d" BoolType
row1 = [IntegerValue 5, IntegerValue 15, StringValue "ddddddddddd", BoolValue True]
row2 :: [Value]
row2 = [IntegerValue 10, IntegerValue 20, StringValue "no", BoolValue False]
testData :: DataFrame
testData = DataFrame [a, b, c, d] [row1, row2]