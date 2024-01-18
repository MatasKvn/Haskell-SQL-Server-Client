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

### Available keywords
  * `SELECT` desired columns
  * `FROM` desired tables
  * `WHERE` desired conditions (int only!)
  * `ORDER BY` desired order
<br></br>

Dependencies can be found in package.yaml
<br></br>
Open any .hs file, Haskell extension should pick up
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
