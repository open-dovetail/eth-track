# Setup standard ERC contracts

Use `abigen` tool to generate Go code for standard ERC contracts, and so we can use the code to fetch contract properties, such as `name`, `symbol`, and `decimals`, from Ethereum nodes.

## install solc

```bash
brew update
brew tap ethereum/ethereum
brew install solidity
solc --version
```

## build ethgo abigen tool

```bash
cd ~/work/ethereum
git clone git@github.com:umbracle/ethgo.git
cd ethgo/cmd
go build -o ethgo
mv ethgo $GOPATH/bin
ethgo --help
```

## generate abi and standard package using web3-abigen

Run the following scripts to generate ABI and Go code from public methods and events of [ERC777](./erc777/artifacts/erc777.sol), which includes definitions of all methods and events for `ERC20`. 

```bash
cd erc777/artifacts
solc --abi -o . erc777.sol
ethgo abigen --source ERC777.abi --package erc777 --output ..
```

Do the same for the other standard contracts, i.e., [ERC721](./erc721/artifacts/erc721.sol) and [ERC1155](./erc1155/artifacts/erc1155.sol).

## Alternative code generation for contracts

Following instruction based on [ref](https://goethereumbook.org/smart-contract-read-erc20/) will generate code using `go-ethereum` abigen, which will generate code differently, and thus is not so convenient to use with `go-web3`.

### build go-ethereum abigen tool

```bash
cd ~/work/ethereum
# install protoc used to build devtools
brew install protobuf
git clone git@github.com:ethereum/go-ethereum.git
cd go-ethereum
make
make devtools
abigen -v
```

### generate abi and standard package

```bash
cd erc777/artifacts
solc --abi -o . erc777.sol
abigen --abi ERC777.abi -pkg erc777 --out ../erc777.go
```