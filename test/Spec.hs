import Data.Either
import Data.Maybe 
import InMemoryTables qualified as D
import Lib1
import Lib2
import Lib3
import DataFrame
import Test.Hspec
import qualified Lib2
import System.Environment

main :: IO ()
main = do 
  -- Set the current environment for TESTING
  setEnv "ENVIRONMENT_TEST" "1"
  hspec $ do

    describe "TEST" $ do
      it "detects test environment" $ do
        isTest <- Lib3.isTestEnv
        isTest `shouldBe` True
        
    describe "Lib3 Parsing" $ do
      it "Parses 'NOW();'" $ do
        Lib3.parseSql "NOW();" `shouldBe` Right Lib3.ShowCurrentTime

      it "Parses 'SELECT'" $ do
        Lib3.parseSql "SELECT a, b FROM c, d WHERE a!=b OR b=1;" `shouldBe` Right (Lib3.SelectStatement ["a","b"] ["c","d"] ["a!=b","b=1"])
      it "Parses 'DELETE'" $ do
        Lib3.parseSql "DELETE FROM a where exampleColumn = 1 OR exampleColumn != 5;" `shouldBe` Right (Lib3.DeleteStatement "a" ["exampleColumn=1","exampleColumn!=5"])
      it "Parses 'INSERT'" $ do
        Lib3.parseSql "INSERT INTO a (a1, a2) VALUES (1, True);" `shouldBe` Right (Lib3.InsertStatement "a" ["a1","a2"] ["1","True"])
      it "Parses 'UPDATE'" $ do
        Lib3.parseSql "Update table SET column1=True WHERE column2=5;" `shouldBe` Right (Lib3.UpdateStatement "table" ["column1=True"] ["column2=5"])
        
    describe "\nLib3 NOW()" $ do
      it "gets current time" $ do
        result <- Lib3.runExecuteIO $ Lib3.executeSql "NOW();"
        result `shouldSatisfy` isRight
    describe "Lib3 SELECT" $ do
      it "executes SELECT queries" $ do
        result <- Lib3.runExecuteIO $ Lib3.executeSql "SELECT value, id FROM flags, employees;"
        result `shouldSatisfy` isRight

    

    describe "\nLib1.findTableByName" $ do
      it "handles empty lists" $ do
        Lib1.findTableByName [] "" `shouldBe` Nothing
      it "handles empty names" $ do
        Lib1.findTableByName D.database "" `shouldBe` Nothing
      it "can find by name" $ do
        Lib1.findTableByName D.database "employees" `shouldBe` Just (snd D.tableEmployees)
      it "can find by case-insensitive name" $ do
        Lib1.findTableByName D.database "employEEs" `shouldBe` Just (snd D.tableEmployees)
    describe "Lib1.parseSelectAllStatement" $ do
      it "handles empty input" $ do
        Lib1.parseSelectAllStatement "" `shouldSatisfy` isLeft
      it "handles invalid queries" $ do
        Lib1.parseSelectAllStatement "select from dual" `shouldSatisfy` isLeft
      it "returns table name from correct queries" $ do
        Lib1.parseSelectAllStatement "selecT * from dual;" `shouldBe` Right "dual"
    describe "Lib1.validateDataFrame" $ do
      it "finds types mismatch" $ do
        Lib1.validateDataFrame (snd D.tableInvalid1) `shouldSatisfy` isLeft
      it "finds column size mismatch" $ do
        Lib1.validateDataFrame (snd D.tableInvalid2) `shouldSatisfy` isLeft
      it "reports different error messages" $ do
        Lib1.validateDataFrame (snd D.tableInvalid1) `shouldNotBe` Lib1.validateDataFrame (snd D.tableInvalid2)
      it "passes valid tables" $ do
        Lib1.validateDataFrame (snd D.tableWithNulls) `shouldBe` Right ()
    describe "Lib1.renderDataFrameAsTable" $ do
      it "renders a table" $ do
        Lib1.renderDataFrameAsTable 100 (snd D.tableEmployees) `shouldSatisfy` not . null

    -- Lib2.hs Tests
    describe "\nLib2.hs SHOW TABLE & SHOW TABLE 'name'" $ do
      it "shows tables" $ do
        Lib2.parseStatement "Show Tables   ; a;sdf;; das;af;sd;;; ;;a" `shouldBe` Right ShowTables
      it "shows table 'name'" $ do
        Lib2.parseStatement "   Show TablE     employees  ;" `shouldBe` Right (ShowTableName "employees")
      it "handles bad 'show tables' input" $ do
        Lib2.parseStatement "show tables effefefe;" `shouldSatisfy` isLeft
      it "handles bad 'show tables name' input" $ do
        Lib2.parseStatement "show tables employees   123478451   ;" `shouldSatisfy` isLeft

    describe "Lib2.hs Parsing" $ do
      it "handles incorrect queries" $ do
        Lib2.parseStatement "" `shouldSatisfy` isLeft
        Lib2.parseStatement ";" `shouldSatisfy` isLeft
        Lib2.parseStatement "select * from;" `shouldSatisfy` isLeft
        Lib2.parseStatement "selct * from;" `shouldSatisfy` isLeft

      it "parses correct queries" $ do
        Lib2.parseStatement "SELECT * FROM employees;" `shouldBe` Right (ColumnList ["*"] [] "employees")
        Lib2.parseStatement "SELECT id, name FROM employees;" `shouldBe` Right (ColumnList ["id", "name"] [] "employees")
      it "parses correct queries with WHERE" $ do
        Lib2.parseStatement "SeLeCt name, surname, name, surname FroM employees wheRE id=1;" `shouldBe` Right (ColumnList ["name", "surname", "name", "surname"] ["id=1"] "employees")
        Lib2.parseStatement "SELECT   *    FROM    employees   WHERE id > 1 OR  id !=5 OR id<> 10;" `shouldBe` Right (ColumnList ["*"] ["id>1", "id!=5", "id<>10"] "employees")
      it "parses correct queries with MIN, SUM" $ do
        Lib2.parseStatement "SELECT MIN(id) FROM employees;" `shouldBe` Right (ColumnList ["MIN(id)"] [] "employees")
        Lib2.parseStatement "SELECT sUm(id) FROM employees;" `shouldBe` Right (ColumnList ["sUm(id)"] [] "employees")
        Lib2.parseStatement "SELECT sum(id), min(name) FROM employees WHERE id>0;" `shouldBe` Right (ColumnList ["sum(id)", "min(name)"] ["id>0"] "employees")

    describe "Lib2.hs Executing ParsedStatemtends" $ do
      it "handles incorrect data" $ do
        Lib2.executeStatement (ColumnList ["iddddddddddd", "name", "surname"] [] "employees") `shouldSatisfy` isLeft
        Lib2.executeStatement (ColumnList ["iddddddddddd", "min(name)", "surname"] ["id=1"] "employees") `shouldSatisfy` isLeft
      it "handles incorrect conditions" $ do
        Lib2.executeStatement (ColumnList ["id"] ["a"] "employees") `shouldSatisfy` isLeft
        Lib2.executeStatement (ColumnList ["id"] ["aaa<=5"] "employees") `shouldSatisfy` isLeft
        Lib2.executeStatement (ColumnList ["id"] [""] "employees") `shouldSatisfy` isLeft
        
      it "executes correct data" $ do
        Lib2.executeStatement (ColumnList ["name", "surname", "name", "surname"] [] "employees") `shouldBe` Right (DataFrame [Column "name" StringType,Column "surname" StringType,Column "name" StringType,Column "surname" StringType] [[StringValue "Vi",StringValue "Po",StringValue "Vi",StringValue "Po"],[StringValue "Ed",StringValue "Dl",StringValue "Ed",StringValue "Dl"]])
      it "executes correct data with 'OR' conditions" $ do
        Lib2.executeStatement (ColumnList ["name", "surname", "name", "surname"] ["id=1"] "employees") `shouldBe` Right (DataFrame [Column "name" StringType,Column "surname" StringType,Column "name" StringType,Column "surname" StringType] [[StringValue "Vi",StringValue "Po",StringValue "Vi",StringValue "Po"]])
        Lib2.executeStatement (ColumnList ["*"] ["id>1", "id!=5", "id<>10"] "employees") `shouldBe` Right (DataFrame [Column "id" IntegerType,Column "name" StringType,Column "surname" StringType] [[IntegerValue 1,StringValue "Vi",StringValue "Po"],[IntegerValue 2,StringValue "Ed",StringValue "Dl"]])  
        Lib2.executeStatement (ColumnList ["SUM(id)", "MIN(name)"] ["id>1"] "employees") `shouldBe` Right (DataFrame [Column "sum" IntegerType,Column "min" StringType] [[IntegerValue 2,StringValue "Ed"]])
      
