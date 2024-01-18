# Haskell SQL Server & Client 

Run server using `fp2023-server`

Run Client using `fp2023-client`
<br></br>

### Possibilities:
- Create & Drop tables
- Select
- Delete
- Insert 
- Update 

### Available keywords & syntax
  * `CREATE TABLE <table name>(<column1 name> <column type>, ...)` available column types: int, string, bool
  * `DROP TABLE <table name>`
  - `SELECT`
    * `SELECT <column1, ...>` 
    * `FROM <table1, ...>` 
    * `WHERE <int condition> OR ...` (int only!), optional
    * `ORDER BY <column> <ASC or DESC>` optional
  - `DELETE`
    * `DELETE FROM <table>` 
    * `WHERE <int condition> OR ...` (int only!)
  - `INSERT`
    * `INSERT INTO <table>(column1, ...)`
    * `VALUES (col1value, ...)`
  - `UPDATE`
    * `UPDATE <table>` 
    * `SET <column1> = <newValue>, ...`
<br></br>

Dependencies can be found in package.yaml
<br></br>
If you use VSCode, open any .hs file, Haskell extension should pick up
[project settings](.vscode/settings.json) and install all dependencies. This might take some
time. If the magic does not happen, please install ghcup components manually:

```
ghcup install stack --set 2.9.3
ghcup install hls --set 2.0.0.1
ghcup install cabal --set 3.6.2.0
ghcup install ghc --set 9.4.5
``````
<br></br>
Run tests with `stack test`
