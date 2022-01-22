-- Decode progress
SELECT * FROM ethdb.progress;

WITH toDateTime('2022-01-01', 'UTC') AS startDate
SELECT
	count(*)
FROM
	ethdb.blocks
WHERE
	BlockTime >= startDate
	AND BlockTime < addDays(startDate, 1)

-- monthly block count
SELECT
	count(*) AS BlockCount,
	avg(Difficulty),
	sum(GasUsed),
	toStartOfMonth(BlockTime) AS BlockMonth
FROM
	ethdb.blocks
GROUP BY
	BlockMonth;

-- or using aggregated view
SELECT
	sum(BlockCount) AS BlockCount,
	max(MaxDifficulty) AS MaxDifficulty,
	sum(TotalGasUsed) AS GasUsed,
	toStartOfMonth(BlockDate) AS BlockMonth
FROM
	ethdb.daily_blocks_by_miner
GROUP BY
	BlockMonth;

-- daily block count by miner
SELECT
	count(*) AS BlockCount,
	Miner,
	toDate(BlockTime) AS BlockDate,
	toStartOfMonth(BlockDate) AS BlockMonth
FROM
	ethdb.blocks
GROUP BY
	Miner,
	BlockDate;

-- or using aggregated view
SELECT
	sum(BlockCount) AS BlockCount,
	Miner,
	BlockDate,
	toStartOfMonth(BlockDate) AS BlockMonth
FROM
	ethdb.daily_blocks_by_miner
GROUP BY
	Miner,
	BlockDate;

-- monthly transaction count
SELECT
	sum(Status) AS TxCount,
	sum(Status * Gas) AS Gas,
	toStartOfMonth(BlockTime) AS TxMonth
FROM
	ethdb.transactions
GROUP BY
	TxMonth;

-- or using aggregated view
SELECT
	sum(TxCount) AS TxCount,
	sum(TotalGas) AS Gas,
	toStartOfMonth(TxDate) AS TxMonth
FROM
	ethdb.daily_transactions_by_contract
GROUP BY
	TxMonth;

-- daily transaction count by contract
SELECT
	sum(Status) AS TxCount,
	`To` AS Contract,
	toDate(BlockTime) AS BlockDate,
	toStartOfMonth(BlockDate) AS TxMonth
FROM
	ethdb.transactions
WHERE
	BlockTime >= toDateTime('2021-12-01','UTC')
	AND BlockTime < addMonths(toDateTime('2021-12-01','UTC'),1)
GROUP BY
	Contract,
	BlockDate;

-- or using aggregated view
SELECT
	sum(TxCount) AS TxCount,
	Contract,
	TxDate,
	toStartOfMonth(TxDate) AS TxMonth
FROM
	ethdb.daily_transactions_by_contract
WHERE 
	TxMonth = '2021-12-01'
--	TxDate >= toDate('2021-12-01','UTC')
--	AND TxDate < addMonths(toDate('2021-12-01','UTC'),1)
GROUP BY
	Contract,
	TxDate;

-- contracts with symbols
SELECT
	DISTINCT Address,
	Symbol,
	Decimals
FROM
	ethdb.contracts
WHERE
	Symbol != ''

-- daily token transfers
SELECT
	sum(Status) AS TxCount,
	sum(Status * arrayElement(`Params.ValueDouble`, 2)) AS TxAmount,
	`To` AS Contract,
	toDate(BlockTime) AS TxDate,
	toStartOfMonth(TxDate) AS TxMonth
FROM
	ethdb.transactions
WHERE
	BlockTime >= toDateTime('2021-12-01','UTC')
	AND BlockTime < addMonths(toDateTime('2021-12-01','UTC'),1)
	AND Contract != ''
	AND Method = 'transfer'
GROUP BY
	Contract,
	TxDate;
	
-- or using aggregated view
SELECT
	sum(TxCount) AS TxCount,
	sum(TxAmount) AS TxAmount,
	Contract,
	TxDate,
	toStartOfMonth(TxDate) AS TxMonth
FROM
	ethdb.daily_token_transfers_by_contract
WHERE 
	TxMonth = '2021-12-01'
GROUP BY
	Contract,
	TxDate;

-- token transfers by symbol
SELECT
	t.TxCount,
	divide(t.TxAmount, exp10(c.Decimals)) AS Amount,
	c.Symbol,
	t.TxDate
FROM (
	SELECT
		sum(Status) AS TxCount,
		sum(Status * arrayElement(`Params.ValueDouble`, 2)) AS TxAmount,
		`To` AS Contract,
		toDate(BlockTime) AS TxDate
	FROM
		ethdb.transactions
	WHERE
		BlockTime >= toDateTime('2021-12-01','UTC')
		AND BlockTime < addMonths(toDateTime('2021-12-01','UTC'),1)
		AND Contract != ''
		AND Method = 'transfer'
	GROUP BY
		Contract,
		TxDate
	ORDER BY
		TxCount DESC
	LIMIT 2000) AS t
INNER JOIN (
	SELECT
		DISTINCT Address,
		Symbol,
		Decimals
	FROM
		ethdb.contracts
) AS c ON
	c.Symbol != ''
	AND t.Contract = c.Address;

-- or use aggregated view
SELECT
	t.TxCount,
	divide(t.TxAmount, exp10(c.Decimals)) AS Amount,
	c.Symbol,
	t.TxDate
FROM (
	SELECT
		sum(TxCount) AS TxCount,
		sum(TxAmount) AS TxAmount,
		Contract,
		TxDate
	FROM
		ethdb.daily_token_transfers_by_contract
	WHERE 
		TxDate >= toDate('2021-12-01','UTC')
		AND TxDate < addMonths(toDate('2021-12-01','UTC'),1)
	GROUP BY
		Contract,
		TxDate
	ORDER BY 
		TxCount DESC
	LIMIT 2000) AS t
INNER JOIN (
	SELECT
		DISTINCT Address,
		Symbol,
		Decimals
	FROM
		ethdb.contracts
) AS c ON
	c.Symbol != ''
	AND t.Contract = c.Address;

-- Token senders monthly summary
SELECT
	sum(Status) AS TxCount,
	sum(Status * arrayElement(`Params.ValueDouble`, 2)) AS TxAmount,
	`To` AS Contract,
	`From` AS Sender,
	toStartOfMonth(BlockTime) AS TxMonth
FROM
	ethdb.transactions
WHERE
	Method = 'transfer'
	AND TxMonth = '2021-12-01'
GROUP BY
	Contract,
	Sender,
	TxMonth;

-- or use aggregated view
SELECT
	sum(TxCount) AS TxCount,
	sum(TxAmount) AS TxAmount,
	Contract,
	Sender,
	TxMonth
FROM
	ethdb.monthly_tokens_sent_by_account
WHERE
	TxMonth = toDate('2021-12-01', 'UTC')
GROUP BY
	TxMonth,
	Contract,
	Sender;

-- Token receipients monthly summary
SELECT
	sum(Status) AS TxCount,
	sum(Status * arrayElement(`Params.ValueDouble`, 2)) AS TxAmount,
	`To` AS Contract,
	substring(arrayElement(`Params.ValueString`, 1), 3) AS Recipient,
	toStartOfMonth(BlockTime) AS TxMonth
FROM
	ethdb.transactions
WHERE
	Method = 'transfer'
	AND TxMonth = '2021-12-01'
GROUP BY
	Contract,
	Recipient,
	TxMonth;

-- or use aggregated view
SELECT
	sum(TxCount) AS TxCount,
	sum(TxAmount) AS TxAmount,
	Contract,
	Recipient,
	TxMonth
FROM
	ethdb.monthly_tokens_received_by_account
WHERE
	TxMonth = toDate('2021-12-01', 'UTC')
GROUP BY
	TxMonth,
	Contract,
	Recipient;

-- Joined token sender and recipient
SELECT
	IF(s.Sender != '', s.Sender, r.Recipient) AS Address,
	s.SendCount,
	r.ReceiveCount
FROM (
	SELECT
		sum(Status) AS SendCount,
		`From` AS Sender
	FROM
		ethdb.transactions
	WHERE
		BlockTime >= toDateTime('2021-12-01','UTC')
		AND BlockTime < addMonths(toDateTime('2021-12-01','UTC'),1)
		AND Method = 'transfer'
	GROUP BY
		Sender
	ORDER BY
		SendCount DESC
	LIMIT 500) AS s
FULL OUTER JOIN (
	SELECT
		sum(Status) AS ReceiveCount,
		substring(arrayElement(`Params.ValueString`, 1), 3) AS Recipient
	FROM
		ethdb.transactions
	WHERE
		BlockTime >= toDateTime('2021-12-01','UTC')
		AND BlockTime < addMonths(toDateTime('2021-12-01','UTC'),1)
		AND Method = 'transfer'
	GROUP BY
		Recipient
	ORDER BY
		ReceiveCount DESC
	LIMIT 500) AS r
ON
	r.Recipient = s.Sender;

-- or use aggregated view
SELECT
	IF(s.Sender != '', s.Sender, r.Recipient) AS Address,
	s.SendCount,
	r.ReceiveCount
FROM (
	SELECT
		sum(TxCount) AS SendCount,
		Sender
	FROM
		ethdb.monthly_tokens_sent_by_account
	WHERE
		TxMonth = toDate('2021-12-01', 'UTC')
	GROUP BY
		Sender
	ORDER BY
		SendCount DESC
	LIMIT 500) AS s
FULL OUTER JOIN (
	SELECT
		sum(TxCount) AS ReceiveCount,
		Recipient
	FROM
		ethdb.monthly_tokens_received_by_account
	WHERE
		TxMonth = toDate('2021-12-01', 'UTC')
	GROUP BY
		Recipient
	ORDER BY
		ReceiveCount DESC
	LIMIT 500) AS r
ON
	r.Recipient = s.Sender;

-- monthly event count
SELECT
	count(*) AS EvtCount,
	toStartOfMonth(BlockTime) AS EvtMonth
FROM
	ethdb.logs
WHERE
	Removed != 1
GROUP BY
	EvtMonth;

-- or use aggregated view
SELECT
	sum(EvtCount) AS EvtCount,
	toStartOfMonth(EvtDate) AS EvtMonth
FROM 
	ethdb.daily_events_by_contract
GROUP BY
	EvtMonth;

-- daily event count by event name
SELECT
	count(*) AS EvtCount,
	Address,
	Event,
	toDate(BlockTime) AS EvtDate,
	toStartOfMonth(EvtDate) AS EvtMonth
FROM
	ethdb.logs
WHERE
	Removed != 1
	AND BlockTime >= toDateTime('2021-12-01','UTC')
	AND BlockTime < addMonths(toDateTime('2021-12-01','UTC'),1)
GROUP BY
	Address,
	Event,
	EvtDate;

-- or use aggregated view
SELECT
	sum(EvtCount) AS EvtCount,
	Contract,
	Event,
	EvtDate,
	toStartOfMonth(EvtDate) AS EvtMonth
FROM ethdb.daily_events_by_contract
WHERE 
	EvtMonth = toDate('2021-12-01', 'UTC')
GROUP BY
	EvtDate,
	Contract,
	Event;

-- daily token transfer events
SELECT
	count(*) AS EvtCount,
	sum(arrayElement(`Params.ValueDouble`, 3)) AS EvtAmount,
	Address,
	toDate(BlockTime) AS EvtDate,
	toStartOfMonth(EvtDate) AS EvtMonth
FROM
	ethdb.logs
WHERE
	BlockTime >= toDateTime('2021-12-01','UTC')
	AND BlockTime < addMonths(toDateTime('2021-12-01','UTC'),1)
	AND Event = 'Transfer'
	AND Address != ''
	AND Removed != 1
GROUP BY
	Address,
	EvtDate;

-- or use aggregated view
SELECT
	sum(EvtCount) AS EvtCount,
	sum(EvtAmount) AS EvtAmount,
	Contract,
	EvtDate,
	toStartOfMonth(EvtDate) AS EvtMonth
FROM 
	ethdb.daily_token_events_by_contract
WHERE
	EvtMonth = toDate('2021-12-01', 'UTC')
GROUP BY 
	EvtDate,
	Contract;

-- token transfer events by symbol
SELECT
	t.EvtCount,
	divide(t.EvtAmount, exp10(c.Decimals)) AS Amount,
	c.Symbol,
	t.EvtDate
FROM (
	SELECT
		count(*) AS EvtCount,
		sum(arrayElement(`Params.ValueDouble`, 3)) AS EvtAmount,
		Address,
		toDate(BlockTime) AS EvtDate
	FROM
		ethdb.logs
	WHERE
		BlockTime >= toDateTime('2021-12-01','UTC')
		AND BlockTime < addMonths(toDateTime('2021-12-01','UTC'),1)
		AND Event = 'Transfer'
		AND Address != ''
		AND Removed != 1
	GROUP BY
		Address,
		EvtDate
	ORDER BY
		EvtCount DESC
	LIMIT 2000) AS t
INNER JOIN (
	SELECT
		DISTINCT Address,
		Symbol,
		Decimals
	FROM
		ethdb.contracts
	WHERE
		Symbol != ''
) AS c ON
	t.Address = c.Address;

-- or use aggregated view
SELECT
	t.EvtCount,
	divide(t.EvtAmount, exp10(c.Decimals)) AS Amount,
	c.Symbol,
	t.EvtDate
FROM (
	SELECT
		sum(EvtCount) AS EvtCount,
		sum(EvtAmount) AS EvtAmount,
		Contract,
		EvtDate
	FROM 
		ethdb.daily_token_events_by_contract
	WHERE
		EvtDate >= toDate('2021-12-01', 'UTC')
		AND EvtDate < addMonths(toDate('2021-12-01','UTC'),1)
	GROUP BY 
		EvtDate,
		Contract
	ORDER BY
		EvtCount DESC
	LIMIT 2000) AS t
INNER JOIN (
	SELECT
		DISTINCT Address,
		Symbol,
		Decimals
	FROM
		ethdb.contracts
	WHERE
		Symbol != ''
) AS c ON
	t.Contract = c.Address;

-- Token sender events monthly summary
SELECT
	count(*) AS SendCount,
	sum(arrayElement(`Params.ValueDouble`, 3)) AS SentAmount,
	Address,
	substring(arrayElement(`Params.ValueString`, 1), 3) AS Sender,
	toStartOfMonth(BlockTime) AS EvtMonth
FROM
	ethdb.logs
WHERE
	Event = 'Transfer'
	AND Removed != 1
	AND EvtMonth = '2021-12-01'
GROUP BY
	Sender,
	Address,
	EvtMonth;

-- or use aggregated view
SELECT
	sum(EvtCount) AS SendCount,
	sum(EvtAmount) AS SendAmount,
	Contract,
	Sender,
	EvtMonth
FROM 
	ethdb.monthly_tokens_sent_event_by_account
WHERE 
	EvtMonth = toDate('2021-12-01', 'UTC')
GROUP BY 
	EvtMonth,
	Contract,
	Sender;

-- Token receiver events monthly summary
SELECT
	count(*) AS ReceiveCount,
	sum(arrayElement(`Params.ValueDouble`, 3)) AS ReceivedAmount,
	Address,
	substring(arrayElement(`Params.ValueString`, 2), 3) AS Recipient,
	toStartOfMonth(BlockTime) AS EvtMonth
FROM
	ethdb.logs
WHERE
	Event = 'Transfer'
	AND Removed != 1
	AND EvtMonth = '2021-12-01'
GROUP BY
	Recipient,
	Address,
	EvtMonth;

-- or use aggregated view
SELECT
	sum(EvtCount) AS ReceiveCount,
	sum(EvtAmount) AS ReceivedAmount,
	Contract,
	Recipient,
	EvtMonth
FROM 
	ethdb.monthly_tokens_received_event_by_account
WHERE 
	EvtMonth = toDate('2021-12-01', 'UTC')
GROUP BY 
	EvtMonth,
	Contract,
	Recipient;

-- Joined token sender and recipient events
SELECT
	IF(s.Sender != '', s.Sender, r.Recipient) AS Address,
	s.SendCount,
	r.ReceiveCount
FROM (
	SELECT
		count(*) AS SendCount,
		substring(arrayElement(`Params.ValueString`, 1), 3) AS Sender
	FROM
		ethdb.logs
	WHERE
		BlockTime >= toDateTime('2021-12-01','UTC')
		AND BlockTime < addMonths(toDateTime('2021-12-01','UTC'),1)
		AND Event = 'Transfer'
		AND Removed != 1
	GROUP BY
		Sender
	ORDER BY
		SendCount DESC
	LIMIT 1000) AS s
FULL OUTER JOIN (
	SELECT
		count(*) AS ReceiveCount,
		substring(arrayElement(`Params.ValueString`, 2), 3) AS Recipient
	FROM
		ethdb.logs
	WHERE
		BlockTime >= toDateTime('2021-12-01','UTC')
		AND BlockTime < addMonths(toDateTime('2021-12-01','UTC'),1)
		AND Event = 'Transfer'
		AND Removed != 1
	GROUP BY
		Recipient
	ORDER BY
		ReceiveCount DESC
	LIMIT 1000
) AS r ON
	r.Recipient = s.Sender
WHERE
	Address != '0000000000000000000000000000000000000000';

-- or use aggregated view
SELECT
	IF(s.Sender != '', s.Sender, r.Recipient) AS Address,
	s.SendCount,
	r.ReceiveCount
FROM (
	SELECT
		sum(EvtCount) AS SendCount,
		Sender
	FROM 
		ethdb.monthly_tokens_sent_event_by_account
	WHERE 
		EvtMonth = toDate('2021-12-01', 'UTC')
		AND Sender != '0000000000000000000000000000000000000000'
	GROUP BY 
		Sender
	ORDER BY
		SendCount DESC
	LIMIT 1000) AS s
FULL OUTER JOIN (
	SELECT
		sum(EvtCount) AS ReceiveCount,
		Recipient
	FROM 
		ethdb.monthly_tokens_received_event_by_account
	WHERE 
		EvtMonth = toDate('2021-12-01', 'UTC')
		AND Recipient != '0000000000000000000000000000000000000000'
	GROUP BY
		Recipient
	ORDER BY
		ReceiveCount DESC
	LIMIT 1000
) AS r ON
	r.Recipient = s.Sender;

-- USDC: a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48
optimize table ethdb.contracts final;
select Address, Symbol, Name, Decimals, ABI from ethdb.contracts where Symbol = 'USDC';

-- daily USDC event summary
SELECT
	count(*) AS EvtCount,
	toInt256(sum(divide(arrayElement(`Params.ValueDouble`, 3), exp10(6)))) as Amount,
	toDate(BlockTime) as EvtDate
FROM
	ethdb.logs
WHERE
	Removed != 1
	AND Address = 'a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'
	AND Event = 'Transfer'
GROUP BY
	EvtDate;

-- or use aggregated view
SELECT
	sum(EvtCount) AS EvtCount,
	divide(sum(EvtAmount), exp10(6)) AS Amount,
	EvtDate
FROM 
	ethdb.daily_token_events_by_contract
WHERE 
	Contract = 'a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'
GROUP BY
	EvtDate;

-- Joined USDC token sender and recipient events
SELECT
	IF(s.Sender != '', s.Sender, r.Recipient) AS Address,
	s.SendCount,
	toInt256(divide(s.SentAmount, exp10(6))) AS SentAmount,
	r.ReceiveCount,
	toInt256(divide(r.ReceivedAmount, exp10(6))) AS ReceivedAmount 
FROM (
	SELECT
		count(*) AS SendCount,
		sum(arrayElement(`Params.ValueDouble`, 3)) as SentAmount,
		substring(arrayElement(`Params.ValueString`, 1), 3) AS Sender
	FROM
		ethdb.logs
	WHERE
		BlockTime >= toDateTime('2021-12-01','UTC')
		AND BlockTime < addMonths(toDateTime('2021-12-01','UTC'),1)
		AND Address = 'a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'
		AND Event = 'Transfer'
		AND Removed != 1
	GROUP BY
		Sender
	ORDER BY
		SentAmount DESC
	LIMIT 500) AS s
FULL OUTER JOIN (
	SELECT
		count(*) AS ReceiveCount,
		sum(arrayElement(`Params.ValueDouble`, 3)) as ReceivedAmount,
		substring(arrayElement(`Params.ValueString`, 2), 3) AS Recipient
	FROM
		ethdb.logs
	WHERE
		BlockTime >= toDateTime('2021-12-01','UTC')
		AND BlockTime < addMonths(toDateTime('2021-12-01','UTC'),1)
		AND Address = 'a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'
		AND Event = 'Transfer'
		AND Removed != 1
	GROUP BY
		Recipient
	ORDER BY
		ReceivedAmount DESC
	LIMIT 500
) AS r ON
	r.Recipient = s.Sender;

-- or use aggregated view
SELECT
	IF(s.Sender != '', s.Sender, r.Recipient) AS Address,
	s.SendCount,
	toInt256(divide(s.SentAmount, exp10(6))) AS SentAmount,
	r.ReceiveCount,
	toInt256(divide(r.ReceivedAmount, exp10(6))) AS ReceivedAmount 
FROM (
	SELECT
		sum(EvtCount) AS SendCount,
		sum(EvtAmount) AS SentAmount,
		Sender
	FROM 
		ethdb.monthly_tokens_sent_event_by_account
	WHERE 
		EvtMonth = toDate('2021-12-01', 'UTC')
		AND Contract = 'a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'
	GROUP BY 
		Sender
	ORDER BY
		SentAmount DESC
	LIMIT 500) AS s
FULL OUTER JOIN (
	SELECT
		sum(EvtCount) AS ReceiveCount,
		sum(EvtAmount) AS ReceivedAmount,
		Recipient
	FROM 
		ethdb.monthly_tokens_received_event_by_account
	WHERE 
		EvtMonth = toDate('2021-12-01', 'UTC')
		AND Contract = 'a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'
	GROUP BY
		Recipient
	ORDER BY
		ReceivedAmount DESC
	LIMIT 500
) AS r ON
	r.Recipient = s.Sender;

-- Check rejected transactions
optimize table ethdb.transactions final;
select count(*) from ethdb.transactions;

SELECT
	count(),
	toDate(BlockTime) AS TxDate
FROM
	ethdb.transactions
WHERE
	Status = -1
GROUP BY
	TxDate
ORDER BY TxDate;

-- Check recent active contracts
SELECT
	count(),
	LastEventDate
FROM
	ethdb.contracts
GROUP BY
	LastEventDate
ORDER BY
	LastEventDate DESC;

-- Transaction count by contract methods
SELECT
	Sum(Status) AS TxCount,
	`To` AS Contract,
	Method,
	toDate(BlockTime) AS TxDate
FROM
	ethdb.transactions
WHERE
	BlockTime >= '2022-01-01 00:00:00'
	AND `To` != ''
	AND Method != ''
	AND Method != 'UNKNOWN'
GROUP BY
	Contract,
	Method,
	TxDate
ORDER BY
	TxCount DESC;

-- Test aggregated views
select 
	sum(BlockCount),
	max(MaxDifficulty),
	sum(TotalGasUsed),
	Miner
from ethdb.daily_blocks_by_miner 
where BlockDate = '2021-07-22'
group by Miner;

select 
	sum(EvtCount),
	Contract,
	Event
from ethdb.daily_events_by_contract
where EvtDate = '2021-07-22'
group by Contract, Event;

select
	sum(EvtCount),
	sum(EvtAmount),
	Contract 
from ethdb.daily_token_events_by_contract
where EvtDate = '2021-07-22'
group by Contract;

SELECT 
	sum(TxCount),
	sum(TxAmount),
	Contract 
from ethdb.daily_token_transfers_by_contract
where TxDate = '2021-07-22'
group by Contract;

SELECT 
	sum(TxCount),
	max(MaxBlockNumber),
	max(MaxGasPrice),
	sum(TotalGas),
	Contract 
from ethdb.daily_transactions_by_contract
where TxDate = '2021-07-22'
group by Contract;

select
	sum(TxCount),
	sum(TxAmount),
	Contract,
	Recipient
from ethdb.monthly_tokens_received_by_account
where TxMonth = '2021-07-01'
group by Contract, Recipient;

select
	sum(EvtCount),
	sum(EvtAmount),
	Contract,
	Recipient
from ethdb.monthly_tokens_received_event_by_account
where EvtMonth = '2021-07-01'
group by Contract, Recipient;

SELECT 
	sum(TxCount),
	sum(TxAmount),
	Contract,
	Sender
from ethdb.monthly_tokens_sent_by_account
where TxMonth = '2021-07-01'
group by Contract, Sender;

SELECT 
	sum(EvtCount),
	sum(EvtAmount),
	Contract,
	Sender
from ethdb.monthly_tokens_sent_event_by_account
where EvtMonth = '2021-07-01'
group by Contract, Sender;

-- system settings
select * from system.settings where name like 'max_mem%';
select * from system.processes;
select * from system.errors;
