# fp-2023

## Setup
1. Fork this repository, checkout the fork. This project uses GitHub Actions haskell workflow,
please preserve its configuration.
2. Install [ghcup](https://www.haskell.org/ghcup/), please note you might need to install
additional packages, as descriped [here](https://www.haskell.org/ghcup/install/). Just agree
with all defaults during the installation. `ghcup` binary should appear in your `PATH` (you
might need to restart your computer).
3. Install (if not already installed) VSCode. When done, add Haskell ("Haskell language support")
extension.
4. Open any .hs file in the checked out (step 1) repository. Haskell extension should pick up
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