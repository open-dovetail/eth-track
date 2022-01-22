------------------------------------
DROP TABLE IF EXISTS ethdb.d_blocks;
CREATE TABLE ethdb.d_blocks
(
    `BlockCount` UInt64,
    `MaxDifficulty` SimpleAggregateFunction(max, Float64),
    `TotalGasUsed` UInt64,
    `Miner` FixedString(40),
    `BlockDate` Date
)
ENGINE = SummingMergeTree
ORDER BY (BlockDate, Miner);

DROP VIEW IF EXISTS ethdb.daily_blocks_by_miner;
CREATE MATERIALIZED VIEW ethdb.daily_blocks_by_miner
TO ethdb.d_blocks AS SELECT
	count(*) AS BlockCount,
	max(Difficulty) AS MaxDifficulty,
	sum(GasUsed) AS TotalGasUsed,
	Miner,
	toDate(BlockTime) AS BlockDate
FROM
	ethdb.blocks
GROUP BY
	BlockDate,
	Miner;

-- initialize daily_blocks_by_miner
INSERT INTO ethdb.d_blocks
SELECT
	count(*) AS BlockCount,
	max(Difficulty) AS MaxDifficulty,
	sum(GasUsed) AS TotalGasUsed,
	Miner,
	toDate(BlockTime) AS BlockDate
FROM
	ethdb.blocks
GROUP BY
	BlockDate,
	Miner;

SELECT count(*) FROM ethdb.daily_blocks_by_miner;

------------------------------------------
DROP TABLE IF EXISTS ethdb.d_transactions;
CREATE TABLE ethdb.d_transactions
(
    `TxCount` Int64,
    `MaxBlockNumber` SimpleAggregateFunction(max, UInt64),
    `MaxGasPrice` SimpleAggregateFunction(max, UInt64),
    `TotalGas` Int64,
    `Contract` FixedString(40),
    `TxDate` Date
)
ENGINE = SummingMergeTree
PARTITION BY toYYYYMM(`TxDate`)
ORDER BY (TxDate, Contract);

DROP VIEW IF EXISTS ethdb.daily_transactions_by_contract;
CREATE MATERIALIZED VIEW ethdb.daily_transactions_by_contract 
TO d_transactions AS SELECT
	sum(Status) AS TxCount,
	max(BlockNumber) AS MaxBlockNumber,
	max(GasPrice) AS MaxGasPrice,
	sum(Status * Gas) AS TotalGas,
	`To` AS Contract,
	toDate(BlockTime) AS TxDate
FROM
	ethdb.transactions
GROUP BY
	TxDate,
	Contract;

-- initialize daily_transactions_by_contract
INSERT INTO ethdb.d_transactions
SELECT
	sum(Status) AS TxCount,
	max(BlockNumber) AS MaxBlockNumber,
	max(GasPrice) AS MaxGasPrice,
	sum(Status * Gas) AS TotalGas,
	`To` AS Contract,
	toDate(BlockTime) AS TxDate
FROM
	ethdb.transactions
WHERE
	BlockTime >= '2021-07-01 00:00:00'
	AND BlockTime < '2021-10-01 00:00:00'
GROUP BY
	TxDate,
	Contract;

SELECT count(*) FROM ethdb.daily_transactions_by_contract;

-----------------------------------------
DROP TABLE IF EXISTS ethdb.dt_transactions;
CREATE TABLE ethdb.dt_transactions
(
    `TxCount` Int64,
    `TxAmount` Float64,
    `Contract` FixedString(40),
    `TxDate` Date
)
ENGINE = SummingMergeTree
PARTITION BY toYYYYMM(`TxDate`)
ORDER BY (TxDate, Contract);
 
DROP VIEW IF EXISTS ethdb.daily_token_transfers_by_contract;
CREATE MATERIALIZED VIEW ethdb.daily_token_transfers_by_contract 
TO ethdb.dt_transactions AS SELECT
	sum(Status) AS TxCount,
	sum(Status * arrayElement(`Params.ValueDouble`, 2)) AS TxAmount,
	`To` AS Contract,
	toDate(BlockTime) AS TxDate
FROM
	ethdb.transactions
WHERE
	Contract != ''
	AND Method = 'transfer'
GROUP BY
	TxDate,
	Contract;

-- initialize daily_token_transfers_by_contract
INSERT INTO ethdb.dt_transactions
SELECT
	sum(Status) AS TxCount,
	sum(Status * arrayElement(`Params.ValueDouble`, 2)) AS TxAmount,
	`To` AS Contract,
	toDate(BlockTime) AS TxDate
FROM
	ethdb.transactions
WHERE
	BlockTime >= '2021-07-01 00:00:00'
	AND BlockTime < '2021-10-01 00:00:00'
	AND Contract != ''
	AND Method = 'transfer'
GROUP BY
	TxDate,
	Contract;
	
SELECT count(*) from ethdb.daily_token_transfers_by_contract;

------------------------------------------------
DROP TABLE IF EXISTS ethdb.mt_sent_transactions;
CREATE TABLE ethdb.mt_sent_transactions
(
    `TxCount` Int64,
    `TxAmount` Float64,
    `Contract` FixedString(40),
    `Sender` FixedString(40),
    `TxMonth` Date
)
ENGINE = SummingMergeTree
ORDER BY (TxMonth, Contract, Sender);

DROP VIEW IF EXISTS ethdb.monthly_tokens_sent_by_account;
CREATE MATERIALIZED VIEW ethdb.monthly_tokens_sent_by_account
TO ethdb.mt_sent_transactions AS SELECT
	sum(Status) AS TxCount,
	sum(Status * arrayElement(`Params.ValueDouble`, 2)) AS TxAmount,
	`To` AS Contract,
	`From` AS Sender,
	toStartOfMonth(BlockTime) AS TxMonth
FROM
	ethdb.transactions
WHERE
	Contract != ''
	AND Method = 'transfer'
GROUP BY
	TxMonth,
	Contract,
	Sender;

-- initialize monthly_tokens_sent_by_account
INSERT INTO ethdb.mt_sent_transactions 
SELECT
	sum(Status) AS TxCount,
	sum(Status * arrayElement(`Params.ValueDouble`, 2)) AS TxAmount,
	`To` AS Contract,
	`From` AS Sender,
	toStartOfMonth(BlockTime) AS TxMonth
FROM
	ethdb.transactions
WHERE
	BlockTime >= '2021-07-01 00:00:00'
	AND BlockTime < '2021-10-01 00:00:00'
	AND Contract != ''
	AND Method = 'transfer'
GROUP BY
	TxMonth,
	Contract,
	Sender;

SELECT count(*) FROM ethdb.monthly_tokens_sent_by_account;

----------------------------------------------------
DROP TABLE IF EXISTS ethdb.mt_received_transactions;
CREATE TABLE ethdb.mt_received_transactions
(
    `TxCount` Int64,
    `TxAmount` Float64,
    `Contract` FixedString(40),
    `Recipient` String,
    `TxMonth` Date
)
ENGINE = SummingMergeTree
ORDER BY (TxMonth, Contract, Recipient);

DROP VIEW IF EXISTS ethdb.monthly_tokens_received_by_account;
CREATE MATERIALIZED VIEW ethdb.monthly_tokens_received_by_account
TO ethdb.mt_received_transactions AS SELECT
	sum(Status) AS TxCount,
	sum(Status * arrayElement(`Params.ValueDouble`, 2)) AS TxAmount,
	`To` AS Contract,
	substring(arrayElement(`Params.ValueString`, 1), 3) AS Recipient,
	toStartOfMonth(BlockTime) AS TxMonth
FROM
	ethdb.transactions
WHERE
	Contract != ''
	AND Method = 'transfer'
GROUP BY
	TxMonth,
	Contract,
	Recipient;

-- initialize monthly_tokens_received_by_account
INSERT INTO ethdb.mt_received_transactions 
SELECT
	sum(Status) AS TxCount,
	sum(Status * arrayElement(`Params.ValueDouble`, 2)) AS TxAmount,
	`To` AS Contract,
	substring(arrayElement(`Params.ValueString`, 1), 3) AS Recipient,
	toStartOfMonth(BlockTime) AS TxMonth
FROM
	ethdb.transactions
WHERE
	BlockTime >= '2021-07-01 00:00:00'
	AND BlockTime < '2021-10-01 00:00:00'
	AND Contract != ''
	AND Method = 'transfer'
GROUP BY
	TxMonth,
	Contract,
	Recipient;

SELECT count(*) FROM ethdb.monthly_tokens_received_by_account;

------------------------------------
DROP TABLE IF EXISTS ethdb.d_events;
CREATE TABLE ethdb.d_events
(
    `EvtCount` UInt64,
    `Contract` FixedString(40),
    `Event` String,
    `EvtDate` Date
)
ENGINE = SummingMergeTree
PARTITION BY toYYYYMM(`EvtDate`)
ORDER BY (EvtDate, Contract, Event);

DROP VIEW IF EXISTS ethdb.daily_events_by_contract;
CREATE MATERIALIZED VIEW ethdb.daily_events_by_contract 
TO ethdb.d_events AS SELECT
	count(*) AS EvtCount,
	Address AS Contract,
	Event,
	toDate(BlockTime) AS EvtDate
FROM
	ethdb.logs
WHERE
	Removed != 1
GROUP BY
	EvtDate,
	Contract,
	Event;

-- initialize daily_events_by_contract
INSERT INTO ethdb.d_events
SELECT
	count(*) AS EvtCount,
	Address AS Contract,
	Event,
	toDate(BlockTime) AS EvtDate
FROM
	ethdb.logs
WHERE
	BlockTime >= '2021-07-01 00:00:00'
	AND BlockTime < '2021-10-01 00:00:00'
	AND Removed != 1
GROUP BY
	EvtDate,
	Contract,
	Event;
	
SELECT count(*) FROM ethdb.daily_events_by_contract;

-------------------------------------
DROP TABLE IF EXISTS ethdb.dt_events;
CREATE TABLE ethdb.dt_events
(
    `EvtCount` UInt64,
    `EvtAmount` Float64,
    `Contract` FixedString(40),
    `EvtDate` Date
)
ENGINE = SummingMergeTree
PARTITION BY toYYYYMM(`EvtDate`)
ORDER BY (EvtDate, Contract);

DROP VIEW IF EXISTS ethdb.daily_token_events_by_contract;
CREATE MATERIALIZED VIEW ethdb.daily_token_events_by_contract 
TO ethdb.dt_events AS SELECT
	count(*) AS EvtCount,
	sum(arrayElement(`Params.ValueDouble`, 3)) AS EvtAmount,
	Address AS Contract,
	toDate(BlockTime) AS EvtDate
FROM
	ethdb.logs
WHERE
	Contract != ''
	AND Event = 'Transfer'
	AND Removed != 1
GROUP BY
	EvtDate,
	Contract;

-- initialize daily_token_events_by_contract
INSERT INTO ethdb.dt_events
SELECT
	count(*) AS EvtCount,
	sum(arrayElement(`Params.ValueDouble`, 3)) AS EvtAmount,
	Address AS Contract,
	toDate(BlockTime) AS EvtDate
FROM
	ethdb.logs
WHERE
	BlockTime >= '2021-07-01 00:00:00'
	AND BlockTime < '2021-10-01 00:00:00'
	AND Contract != ''	
	AND Event = 'Transfer'
	AND Removed != 1
GROUP BY
	EvtDate,
	Contract;

SELECT count(*) FROM ethdb.daily_token_events_by_contract;

------------------------------------------
DROP TABLE IF EXISTS ethdb.mt_sent_events;
CREATE TABLE ethdb.mt_sent_events
(
    `EvtCount` UInt64,
    `EvtAmount` Float64,
    `Contract` FixedString(40),
    `Sender` String,
    `EvtMonth` Date
)
ENGINE = SummingMergeTree
ORDER BY (EvtMonth, Contract, Sender);

DROP VIEW IF EXISTS ethdb.monthly_tokens_sent_event_by_account;
CREATE MATERIALIZED VIEW ethdb.monthly_tokens_sent_event_by_account
TO ethdb.mt_sent_events AS SELECT
	count(*) AS EvtCount,
	sum(arrayElement(`Params.ValueDouble`, 3)) AS EvtAmount,
	Address AS Contract,
	substring(arrayElement(`Params.ValueString`, 1), 3) AS Sender,
	toStartOfMonth(BlockTime) AS EvtMonth
FROM
	ethdb.logs
WHERE
	Contract != ''
	AND Event = 'Transfer'
	AND Removed != 1
GROUP BY
	EvtMonth,
	Contract,
	Sender;

-- monthly_tokens_sent_event_by_account
INSERT INTO ethdb.mt_sent_events
SELECT
	count(*) AS EvtCount,
	sum(arrayElement(`Params.ValueDouble`, 3)) AS EvtAmount,
	Address AS Contract,
	substring(arrayElement(`Params.ValueString`, 1), 3) AS Sender,
	toStartOfMonth(BlockTime) AS EvtMonth
FROM
	ethdb.logs
WHERE
	BlockTime >= '2021-07-01 00:00:00'
	AND BlockTime < '2021-10-01 00:00:00'
	AND Contract != ''
	AND Event = 'Transfer'
	AND Removed != 1
GROUP BY
	EvtMonth,
	Contract,
	Sender;

SELECT count(*) FROM ethdb.monthly_tokens_sent_event_by_account;

----------------------------------------------
DROP TABLE IF EXISTS ethdb.mt_received_events;
CREATE TABLE ethdb.mt_received_events
(
    `EvtCount` UInt64,
    `EvtAmount` Float64,
    `Contract` FixedString(40),
    `Recipient` String,
    `EvtMonth` Date
)
ENGINE = SummingMergeTree
ORDER BY (EvtMonth, Contract, Recipient);

DROP VIEW IF EXISTS ethdb.monthly_tokens_received_event_by_account;
CREATE MATERIALIZED VIEW ethdb.monthly_tokens_received_event_by_account
TO ethdb.mt_received_events AS SELECT
	count(*) AS EvtCount,
	sum(arrayElement(`Params.ValueDouble`, 3)) AS EvtAmount,
	Address AS Contract,
	substring(arrayElement(`Params.ValueString`, 2), 3) AS Recipient,
	toStartOfMonth(BlockTime) AS EvtMonth
FROM
	ethdb.logs
WHERE
	Contract != ''
	AND Event = 'Transfer'
	AND Removed != 1
GROUP BY
	EvtMonth,
	Contract,
	Recipient;
	
-- initialize monthly_tokens_received_event_by_account
INSERT INTO ethdb.mt_received_events
SELECT
	count(*) AS EvtCount,
	sum(arrayElement(`Params.ValueDouble`, 3)) AS EvtAmount,
	Address AS Contract,
	substring(arrayElement(`Params.ValueString`, 2), 3) AS Recipient,
	toStartOfMonth(BlockTime) AS EvtMonth
FROM
	ethdb.logs
WHERE
	BlockTime >= '2021-07-01 00:00:00'
	AND BlockTime < '2021-10-01 00:00:00'
	AND Contract != ''
	AND Event = 'Transfer'
	AND Removed != 1
GROUP BY
	EvtMonth,
	Contract,
	Recipient;

SELECT count(*) FROM ethdb.monthly_tokens_received_event_by_account;
