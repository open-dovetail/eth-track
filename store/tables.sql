CREATE DATABASE IF NOT EXISTS ethdb;

DROP TABLE IF EXISTS ethdb.progress;
CREATE TABLE ethdb.progress
(
    `ProcessID` Int16,
    `HiBlock` UInt64,
    `LowBlock` UInt64,
    `HiBlockTime` DateTime('UTC'),
    `LowBlockTime` DateTime('UTC')
) ENGINE = ReplacingMergeTree()
ORDER BY (ProcessID);

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
    `LastErrorTime` DateTime,
    `ABI` String
) ENGINE = ReplacingMergeTree()
ORDER BY (Address);

DROP TABLE IF EXISTS ethdb.blocks;
CREATE TABLE ethdb.blocks
(
    `Hash` FixedString(64),
    `Number` UInt64,
    `ParentHash` FixedString(64),
    `Miner` FixedString(40),
    `Difficulty` Float64,
    `GasLimit` UInt64,
    `GasUsed` UInt64,
    `Status` Int8,
    `BlockTime` DateTime('UTC')
) ENGINE = CollapsingMergeTree(Status)
PARTITION BY toYYYYMM(BlockTime)
ORDER BY (Number);

DROP TABLE IF EXISTS ethdb.transactions;
CREATE TABLE ethdb.transactions
(
    `Hash` FixedString(64),
    `BlockNumber` UInt64,
    `TxnIndex` UInt64,
    `Status` Int8,
    `From` FixedString(40),
    `To` FixedString(40),
    `Method` String,
    `Params` Nested(
        Name String,
        Seq Int8,
        ValueString String,
        ValueDouble Float64),
    `GasPrice` UInt64,
    `Gas` UInt64,
    `Value` Float64,
    `Nonce` UInt64,
    `BlockTime` DateTime('UTC')
) ENGINE = CollapsingMergeTree(Status)
PARTITION BY toYYYYMM(BlockTime)
ORDER BY (To, BlockTime, Hash);

DROP TABLE IF EXISTS ethdb.logs;
CREATE TABLE ethdb.logs
(
    `BlockNumber` UInt64,
    `LogIndex` UInt64,
    `Removed` Int8,
    `TxnIndex` UInt64,
    `TxnHash` FixedString(64),
    `Address` FixedString(40),
    `Event` String,
    `Params` Nested(
        Name String,
        Seq Int8,
        ValueString String,
        ValueDouble Float64),
    `BlockTime` DateTime('UTC')
) ENGINE = CollapsingMergeTree(Removed)
PARTITION BY toYYYYMM(BlockTime)
ORDER BY (Address, BlockTime, BlockNumber, LogIndex);

DROP VIEW IF EXISTS ethdb.tx_view;
CREATE VIEW ethdb.tx_view AS 
    SELECT Hash, BlockNumber, TxnIndex, Status, From, To, Method, GasPrice, Gas, Value, Nonce, BlockTime 
    FROM ethdb.transactions;

DROP VIEW IF EXISTS ethdb.log_view;
CREATE VIEW ethdb.log_view AS 
    SELECT BlockNumber, LogIndex, Removed, TxnIndex, TxnHash, Address, Event, BlockTime 
    FROM ethdb.logs;

DROP VIEW IF EXISTS ethdb.tx_number_params;
CREATE VIEW ethdb.tx_number_params AS 
    SELECT Hash, Status, From, To, Method, Params.Name, Params.ValueDouble, BlockTime 
    FROM ethdb.transactions 
    ARRAY JOIN Params 
    WHERE Method != '' AND Method != 'UNKNOWN' AND Params.ValueDouble > 0;

DROP VIEW IF EXISTS ethdb.tx_string_params;
CREATE VIEW ethdb.tx_string_params AS 
    SELECT Hash, Status, From, To, Method, Params.Name, Params.ValueString, BlockTime 
    FROM ethdb.transactions 
    ARRAY JOIN Params 
    WHERE Method != '' AND Method != 'UNKNOWN' AND Params.ValueString != '';

DROP VIEW IF EXISTS ethdb.log_number_params;
CREATE VIEW ethdb.log_number_params AS
    SELECT BlockNumber, LogIndex, Removed, TxnHash, Address, Event, Params.Name, Params.ValueDouble, BlockTime 
    FROM ethdb.logs 
    ARRAY JOIN Params 
    WHERE Event != 'UNKNOWN' AND Params.ValueDouble > 0;

DROP VIEW IF EXISTS ethdb.log_string_params;
CREATE VIEW ethdb.log_string_params AS
    SELECT BlockNumber, LogIndex, Removed, TxnHash, Address, Event, Params.Name, Params.ValueString, BlockTime 
    FROM ethdb.logs 
    ARRAY JOIN Params 
    WHERE Event != 'UNKNOWN' AND Params.ValueString != '';
