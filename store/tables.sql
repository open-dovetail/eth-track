CREATE DATABASE IF NOT EXISTS ethdb;

DROP TABLE IF EXISTS ethdb.contracts;
CREATE TABLE ethdb.contracts
(
    `Address` FixedString(40),
    `Name` String NULL,
    `Symbol` String NULL,
    `Decimals` UInt8,
    `TotalSupply` Float64,
    `UpdatedDate` Date,
    `StartEventDate` Date,
    `LastEventDate` Date,
    `ABI` String
) ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM(LastEventDate)
ORDER BY (Address);

DROP TABLE IF EXISTS ethdb.blocks;
CREATE TABLE ethdb.blocks
(
    `Hash` FixedString(64),
    `Number` UInt64,
    `ParentHash` FixedString(64),
    `Miner` String,
    `Difficulty` Float64,
    `GasLimit` UInt64,
    `GasUsed` UInt64,
    `BlockTime` DateTime
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(BlockTime)
ORDER BY (BlockTime, Number);

DROP TABLE IF EXISTS ethdb.transactions;
CREATE TABLE ethdb.transactions
(
    `Hash` FixedString(64),
    `BlockNumber` UInt64,
    `TxnIndex` UInt64,
    `Status` UInt8,
    `From` FixedString(40),
    `To` FixedString(40),
    `Method` String,
    `Params` Nested(
        Name String,
        Seq UInt8,
        ValueString String,
        ValueDouble Float64),
    `GasPrice` UInt64,
    `Gas` UInt64,
    `Value` Float64,
    `Nonce` UInt64,
    `BlockTime` DateTime
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(BlockTime)
ORDER BY (To, BlockTime, Hash);

DROP TABLE IF EXISTS ethdb.logs;
CREATE TABLE ethdb.logs
(
    `BlockNumber` UInt64,
    `LogIndex` UInt64,
    `Removed` UInt8,
    `TxnIndex` UInt64,
    `TxnHash` FixedString(64),
    `ContractAddr` FixedString(40),
    `Event` String,
    `Params` Nested(
        Name String,
        Seq UInt8,
        ValueString String,
        ValueDouble Float64),
    `BlockTime` DateTime
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(BlockTime)
ORDER BY (ContractAddr, BlockTime, BlockNumber, LogIndex);
