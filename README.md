# fp-2023


# Task 4

Run server using `fp2023-server`

Run Client using `fp2023-client`

Requirements:
 1. CREATE & DROP table
 2. ORDER BY (ASC, DESC) (or Primary & Foreign keys)
 3. PARSING SQL (State + EitherT)
 4. Thread for saving files
 5. HTTP Server & HTTP Client
 6. YAML serialization for DataFrames

Client: 
- Nuskaito statementus (String)
- Siuncia Statementus (String) i sereri
- Gauna is serverio DataFrame seriaziluota i YAML
- Deseriaziluoja YAML i DataFrame
- Atspausdina DataFrame su renderDataFrameAsTable

Server:
- Laikyt DataFrame'us kaip failus
- Paleidus uzloadint DataFrame is failu
- Paleist thread'a kuris periodiskai issaugo uzloadintus DataFrame'us i failus
- Klauso requestu is Client'u
- Parsina gautus Query'us i ParsedStatement
- Ivykdo ParsedStatement 
- Siuncia Client'ui serializuota DataFrame (YAML)



# Old README.md

## Setup
1. Checkout the repository. This project uses GitHub Actions haskell workflow,
please preserve its configuration.
2. Now you have two options
  - Use GitHub Codespaces (Code -> Codespaces) to develop directly in browser. This is paid
  GitHub feature, but: you get a few compute hours for free and you can get even more if you
  register as student.
  - Use your computer:
    - Install [ghcup](https://www.haskell.org/ghcup/), please note you might need to install
      additional packages, as descriped [here](https://www.haskell.org/ghcup/install/). Just agree
      with all defaults during the installation. `ghcup` binary should appear in your `PATH` (you
      might need to restart your computer).
    - Install (if not already installed) VSCode. When done, add Haskell ("Haskell language support")
      extension.
3. Open any .hs file in the checked out (step 1) repository. Haskell extension should pick up
[project settings](.vscode/settings.json) and install all dependencies. This might take some
time. If the magic does not happen, please install ghcup components manually:

```
ghcup install stack --set 2.9.3
ghcup install hls --set 2.0.0.1
ghcup install cabal --set 3.6.2.0
ghcup install ghc --set 9.4.5
```

# Task 1

Please edit [Lib1](src/Lib1.hs) module (only!).

Run your application: `stack run fp2023-select-all`

Run tests: `stack test`

# Task 2

Please edit [Lib2](src/Lib2.hs) module (only!).

Run your application: `stack run fp2023-select-more`

Add more and run tests: `stack test`

# Task 3

Please edit [Lib3](src/Lib3.hs) and [Main](app3/Main.hs) modules. You can add libraries to [package.yaml](package.yaml).

Run your application: `stack run fp2023-manipulate`

Add more and run tests: `stack test`
