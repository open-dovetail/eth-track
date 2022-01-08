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
    SELECT
		Hash,
		BlockNumber,
		TxnIndex,
		Status,
		`From` AS Sender,
		`To` AS Contract,
		Method,
		GasPrice,
		Gas,
		Value,
		Nonce,
		BlockTime
	FROM
		ethdb.transactions;

DROP VIEW IF EXISTS ethdb.tx_view_1;
CREATE VIEW ethdb.tx_view_1 AS
    SELECT
		Hash,
		Status,
		`From` AS Sender,
		`To` AS Contract,
		Method,
		arrayElement(Params.Name,1) AS Key1,
		arrayElement(Params.ValueString,1) AS Value1,
		arrayElement(Params.ValueDouble,1) AS Amount1,
		Value,
		BlockTime
	FROM
		ethdb.transactions
	WHERE
		Method != ''
		AND Method != 'UNKNOWN'
		AND length(`Params.Name`) = 1;
	
DROP VIEW IF EXISTS ethdb.tx_view_2;
CREATE VIEW ethdb.tx_view_2 AS 
    SELECT
		Hash,
		Status,
		`From` AS Sender,
		`To` AS Contract,
		Method,
		arrayElement(Params.Name,1) AS Key1,
		arrayElement(Params.ValueString,1) AS Value1,
		arrayElement(Params.ValueDouble,1) AS Amount1,
		arrayElement(Params.Name,2) AS Key2,
		arrayElement(Params.ValueString,2) AS Value2,
		arrayElement(Params.ValueDouble,2) AS Amount2,
		Value,
		BlockTime
	FROM
		ethdb.transactions
	WHERE
		Method != ''
		AND Method != 'UNKNOWN'
		AND length(`Params.Name`) = 2;
	
DROP VIEW IF EXISTS ethdb.tx_view_3;
CREATE VIEW ethdb.tx_view_3 AS
    SELECT
		Hash,
		Status,
		`From` AS Sender,
		`To` AS Contract,
		Method,
		arrayElement(Params.Name,1) AS Key1,
		arrayElement(Params.ValueString,1) AS Value1,
		arrayElement(Params.ValueDouble,1) AS Amount1,
		arrayElement(Params.Name,2) AS Key2,
		arrayElement(Params.ValueString,2) AS Value2,
		arrayElement(Params.ValueDouble,2) AS Amount2,
		arrayElement(Params.Name,3) AS Key3,
		arrayElement(Params.ValueString,3) AS Value3,
		arrayElement(Params.ValueDouble,3) AS Amount3,
		Value,
		BlockTime
	FROM
		ethdb.transactions
	WHERE
		Method != ''
		AND Method != 'UNKNOWN'
		AND length(`Params.Name`) = 3;

DROP VIEW IF EXISTS ethdb.tx_view_4;
CREATE VIEW ethdb.tx_view_4 AS
    SELECT
		Hash,
		Status,
		`From` AS Sender,
		`To` AS Contract,
		Method,
		arrayElement(Params.Name,1) AS Key1,
		arrayElement(Params.ValueString,1) AS Value1,
		arrayElement(Params.ValueDouble,1) AS Amount1,
		arrayElement(Params.Name,2) AS Key2,
		arrayElement(Params.ValueString,2) AS Value2,
		arrayElement(Params.ValueDouble,2) AS Amount2,
		arrayElement(Params.Name,3) AS Key3,
		arrayElement(Params.ValueString,3) AS Value3,
		arrayElement(Params.ValueDouble,3) AS Amount3,
		arrayElement(Params.Name,4) AS Key4,
		arrayElement(Params.ValueString,4) AS Value4,
		arrayElement(Params.ValueDouble,4) AS Amount4,
		Value,
		BlockTime
	FROM
		ethdb.transactions
	WHERE
		Method != ''
		AND Method != 'UNKNOWN'
		AND length(`Params.Name`) = 4;

DROP VIEW IF EXISTS ethdb.tx_view_5;
CREATE VIEW ethdb.tx_view_5 AS
    SELECT
		Hash,
		Status,
		`From` AS Sender,
		`To` AS Contract,
		Method,
		arrayElement(Params.Name,1) AS Key1,
		arrayElement(Params.ValueString,1) AS Value1,
		arrayElement(Params.ValueDouble,1) AS Amount1,
		arrayElement(Params.Name,2) AS Key2,
		arrayElement(Params.ValueString,2) AS Value2,
		arrayElement(Params.ValueDouble,2) AS Amount2,
		arrayElement(Params.Name,3) AS Key3,
		arrayElement(Params.ValueString,3) AS Value3,
		arrayElement(Params.ValueDouble,3) AS Amount3,
		arrayElement(Params.Name,4) AS Key4,
		arrayElement(Params.ValueString,4) AS Value4,
		arrayElement(Params.ValueDouble,4) AS Amount4,
		arrayElement(Params.Name,5) AS Key5,
		arrayElement(Params.ValueString,5) AS Value5,
		arrayElement(Params.ValueDouble,5) AS Amount5,
		Value,
		BlockTime
	FROM
		ethdb.transactions
	WHERE
		Method != ''
		AND Method != 'UNKNOWN'
		AND length(`Params.Name`) = 5;

DROP VIEW IF EXISTS ethdb.tx_view_6;
CREATE VIEW ethdb.tx_view_6 AS
    SELECT
		Hash,
		Status,
		`From` AS Sender,
		`To` AS Contract,
		Method,
		arrayElement(Params.Name,1) AS Key1,
		arrayElement(Params.ValueString,1) AS Value1,
		arrayElement(Params.ValueDouble,1) AS Amount1,
		arrayElement(Params.Name,2) AS Key2,
		arrayElement(Params.ValueString,2) AS Value2,
		arrayElement(Params.ValueDouble,2) AS Amount2,
		arrayElement(Params.Name,3) AS Key3,
		arrayElement(Params.ValueString,3) AS Value3,
		arrayElement(Params.ValueDouble,3) AS Amount3,
		arrayElement(Params.Name,4) AS Key4,
		arrayElement(Params.ValueString,4) AS Value4,
		arrayElement(Params.ValueDouble,4) AS Amount4,
		arrayElement(Params.Name,5) AS Key5,
		arrayElement(Params.ValueString,5) AS Value5,
		arrayElement(Params.ValueDouble,5) AS Amount5,
		arrayElement(Params.Name,6) AS Key6,
		arrayElement(Params.ValueString,6) AS Value6,
		arrayElement(Params.ValueDouble,6) AS Amount6,
		Value,
		BlockTime
	FROM
		ethdb.transactions
	WHERE
		Method != ''
		AND Method != 'UNKNOWN'
		AND length(`Params.Name`) = 6;

DROP VIEW IF EXISTS ethdb.tx_view_7;
CREATE VIEW ethdb.tx_view_7 AS
    SELECT
		Hash,
		Status,
		`From` AS Sender,
		`To` AS Contract,
		Method,
		arrayElement(Params.Name,1) AS Key1,
		arrayElement(Params.ValueString,1) AS Value1,
		arrayElement(Params.ValueDouble,1) AS Amount1,
		arrayElement(Params.Name,2) AS Key2,
		arrayElement(Params.ValueString,2) AS Value2,
		arrayElement(Params.ValueDouble,2) AS Amount2,
		arrayElement(Params.Name,3) AS Key3,
		arrayElement(Params.ValueString,3) AS Value3,
		arrayElement(Params.ValueDouble,3) AS Amount3,
		arrayElement(Params.Name,4) AS Key4,
		arrayElement(Params.ValueString,4) AS Value4,
		arrayElement(Params.ValueDouble,4) AS Amount4,
		arrayElement(Params.Name,5) AS Key5,
		arrayElement(Params.ValueString,5) AS Value5,
		arrayElement(Params.ValueDouble,5) AS Amount5,
		arrayElement(Params.Name,6) AS Key6,
		arrayElement(Params.ValueString,6) AS Value6,
		arrayElement(Params.ValueDouble,6) AS Amount6,
		arrayElement(Params.Name,7) AS Key7,
		arrayElement(Params.ValueString,7) AS Value7,
		arrayElement(Params.ValueDouble,7) AS Amount7,
		Value,
		BlockTime
	FROM
		ethdb.transactions
	WHERE
		Method != ''
		AND Method != 'UNKNOWN'
		AND length(`Params.Name`) = 7;

DROP VIEW IF EXISTS ethdb.tx_params_view;
CREATE VIEW ethdb.tx_params_view AS
    SELECT
		Hash,
		Status,
		`From` AS Sender,
		`To` AS Contract,
		Method,
		Params.Name,
		Params.Seq,
		Params.ValueString,
		Params.ValueDouble,
		Value,
		BlockTime
	FROM
		ethdb.transactions
    ARRAY JOIN Params
	WHERE
		Method != ''
		AND Method != 'UNKNOWN'
		AND (Params.ValueString != '' OR Params.ValueDouble > 0);

DROP VIEW IF EXISTS ethdb.log_view;
CREATE VIEW ethdb.log_view AS 
    SELECT
		BlockNumber,
		LogIndex,
		Removed,
		TxnIndex,
		TxnHash,
		Address AS Contract,
		Event,
		BlockTime
	FROM
		ethdb.logs;
	
DROP VIEW IF EXISTS ethdb.log_view_1;
CREATE VIEW ethdb.log_view_1 AS 
    SELECT
		BlockNumber,
		LogIndex,
		Removed,
		TxnIndex,
		TxnHash,
		Address AS Contract,
		Event,
		arrayElement(Params.Name,1) AS Key1,
		arrayElement(Params.ValueString,1) AS Value1,
		arrayElement(Params.ValueDouble,1) AS Amount1,
		BlockTime
	FROM
		ethdb.logs
	WHERE
		Event != ''
		AND length(`Params.Name`) = 1;

DROP VIEW IF EXISTS ethdb.log_view_2;
CREATE VIEW ethdb.log_view_2 AS 
    SELECT
		BlockNumber,
		LogIndex,
		Removed,
		TxnIndex,
		TxnHash,
		Address AS Contract,
		Event,
		arrayElement(Params.Name,1) AS Key1,
		arrayElement(Params.ValueString,1) AS Value1,
		arrayElement(Params.ValueDouble,1) AS Amount1,
		arrayElement(Params.Name,2) AS Key2,
		arrayElement(Params.ValueString,2) AS Value2,
		arrayElement(Params.ValueDouble,2) AS Amount2,
		BlockTime
	FROM
		ethdb.logs
	WHERE
		Event != ''
		AND length(`Params.Name`) = 2;

DROP VIEW IF EXISTS ethdb.log_view_3;
CREATE VIEW ethdb.log_view_3 AS 
    SELECT
		BlockNumber,
		LogIndex,
		Removed,
		TxnIndex,
		TxnHash,
		Address AS Contract,
		Event,
		arrayElement(Params.Name,1) AS Key1,
		arrayElement(Params.ValueString,1) AS Value1,
		arrayElement(Params.ValueDouble,1) AS Amount1,
		arrayElement(Params.Name,2) AS Key2,
		arrayElement(Params.ValueString,2) AS Value2,
		arrayElement(Params.ValueDouble,2) AS Amount2,
		arrayElement(Params.Name,3) AS Key3,
		arrayElement(Params.ValueString,3) AS Value3,
		arrayElement(Params.ValueDouble,3) AS Amount3,
		BlockTime
	FROM
		ethdb.logs
	WHERE
		Event != ''
		AND length(`Params.Name`) = 3;

DROP VIEW IF EXISTS ethdb.log_view_4;
CREATE VIEW ethdb.log_view_4 AS 
    SELECT
		BlockNumber,
		LogIndex,
		Removed,
		TxnIndex,
		TxnHash,
		Address AS Contract,
		Event,
		arrayElement(Params.Name,1) AS Key1,
		arrayElement(Params.ValueString,1) AS Value1,
		arrayElement(Params.ValueDouble,1) AS Amount1,
		arrayElement(Params.Name,2) AS Key2,
		arrayElement(Params.ValueString,2) AS Value2,
		arrayElement(Params.ValueDouble,2) AS Amount2,
		arrayElement(Params.Name,3) AS Key3,
		arrayElement(Params.ValueString,3) AS Value3,
		arrayElement(Params.ValueDouble,3) AS Amount3,
		arrayElement(Params.Name,4) AS Key4,
		arrayElement(Params.ValueString,4) AS Value4,
		arrayElement(Params.ValueDouble,4) AS Amount4,
		BlockTime
	FROM
		ethdb.logs
	WHERE
		Event != ''
		AND length(`Params.Name`) = 4;

DROP VIEW IF EXISTS ethdb.log_view_5;
CREATE VIEW ethdb.log_view_5 AS 
    SELECT
		BlockNumber,
		LogIndex,
		Removed,
		TxnIndex,
		TxnHash,
		Address AS Contract,
		Event,
		arrayElement(Params.Name,1) AS Key1,
		arrayElement(Params.ValueString,1) AS Value1,
		arrayElement(Params.ValueDouble,1) AS Amount1,
		arrayElement(Params.Name,2) AS Key2,
		arrayElement(Params.ValueString,2) AS Value2,
		arrayElement(Params.ValueDouble,2) AS Amount2,
		arrayElement(Params.Name,3) AS Key3,
		arrayElement(Params.ValueString,3) AS Value3,
		arrayElement(Params.ValueDouble,3) AS Amount3,
		arrayElement(Params.Name,4) AS Key4,
		arrayElement(Params.ValueString,4) AS Value4,
		arrayElement(Params.ValueDouble,4) AS Amount4,
		arrayElement(Params.Name,5) AS Key5,
		arrayElement(Params.ValueString,5) AS Value5,
		arrayElement(Params.ValueDouble,5) AS Amount5,
		BlockTime
	FROM
		ethdb.logs
	WHERE
		Event != ''
		AND length(`Params.Name`) = 5;

DROP VIEW IF EXISTS ethdb.log_params_view;
CREATE VIEW ethdb.log_params_view AS
    SELECT
		BlockNumber,
		LogIndex,
		Removed,
		TxnIndex,
		TxnHash,
		Address AS Contract,
		Event,
		Params.Name,
		Params.Seq,
		Params.ValueString,
		Params.ValueDouble,
		BlockTime
	FROM
		ethdb.logs 
    ARRAY JOIN Params
	WHERE
		Event != 'UNKNOWN'
		AND (Params.ValueString != '' OR Params.ValueDouble > 0);
