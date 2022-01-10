-- Decode progress
SELECT * FROM ethdb.progress;
	
-- Check rejected transactions
optimize table ethdb.transactions final;
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
SELECT
	count(),
	TxDate
FROM (
	SELECT
		sum(Status) AS TxCount,
		Hash,
		toDate(BlockTime) AS TxDate
	FROM
		ethdb.transactions
	WHERE
		TxDate = '2021-11-17'
	GROUP BY
		Hash,
		TxDate)
WHERE
	TxCount = 0
GROUP BY
	TxDate;


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

-- Block miners
SELECT
	count() AS BlockCount,
	Miner,
	toDate(BlockTime) AS BlockDate
FROM
	ethdb.blocks
GROUP BY
	Miner,
	BlockDate
ORDER BY
	BlockCount DESC;
-- Equivalent query using block_view
SELECT
	count() AS BlockCount,
	Miner,
	BlockDate
FROM
	ethdb.block_view
GROUP BY
	Miner,
	BlockDate
ORDER BY
	BlockCount DESC;

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
-- Equivalent query using tx_view
SELECT
	Sum(Status) AS TxCount,
	Contract,
	Method,
	BlockDate
FROM
	ethdb.tx_view
WHERE
	BlockDate >= '2022-01-01'
	AND Contract != ''
	AND Method != ''
	AND Method != 'UNKNOWN'
GROUP BY
	Contract,
	Method,
	BlockDate
ORDER BY
	TxCount DESC;

-- Test transaction views
SELECT * FROM ethdb.tx_params_view;
SELECT * FROM ethdb.tx_view;
SELECT * FROM ethdb.tx_view_1;
SELECT * FROM ethdb.tx_view_2;
SELECT * FROM ethdb.tx_view_3;
SELECT * FROM ethdb.tx_view_4;
SELECT * FROM ethdb.tx_view_5;
SELECT * FROM ethdb.tx_view_6;
SELECT * FROM ethdb.tx_view_7;

-- Token transfer total by symbol and date
SELECT
	sum(t.Status) as TxCount,
	sum(t.Status * arrayElement(t.`Params.ValueDouble`, 2) * power(10, -c.Decimals)) as Amount,
	c.Symbol,
	toDate(t.BlockTime) as TxDate
FROM
	ethdb.transactions t
INNER JOIN ethdb.contracts c ON
	t.BlockTime >= '2022-01-01 00:00:00'
	AND t.BlockTime < '2022-01-04 00:00:00'
	AND t.Method = 'transfer'
	AND c.Symbol != ''
	AND t.`To` = c.Address
GROUP BY
	c.Symbol,
	TxDate
ORDER BY
	TxCount DESC
LIMIT 5000;
-- Equivalent query using tx_view_2
SELECT
	sum(t.Status) as TxCount,
	sum(t.Status * t.Amount2 * power(10, -c.Decimals)) as Amount,
	c.Symbol,
	t.BlockDate
FROM
	ethdb.tx_view_2 t
INNER JOIN ethdb.contracts c ON
	t.BlockDate >= '2022-01-01'
	AND t.BlockDate < '2022-01-04'
	AND t.Method = 'transfer'
	AND c.Symbol != ''
	AND t.Contract = c.Address
GROUP BY
	c.Symbol,
	t.BlockDate
ORDER BY
	TxCount DESC
LIMIT 5000;

-- Token transfer total by symbol, sender and date
SELECT
	sum(t.Status) AS TxCount,
	sum(divide(t.Status * arrayElement(t.`Params.ValueDouble`, 2), exp10(c.Decimals))) AS Amount,
	c.Symbol,
	t.`From` AS Sender,
	toDate(t.BlockTime) AS TxDate
FROM
	ethdb.transactions t
INNER JOIN ethdb.contracts c ON
	t.BlockTime >= '2022-01-01 00:00:00'
	AND t.BlockTime < '2022-01-02 00:00:00'
	AND t.Method = 'transfer'
	AND c.Symbol != ''
	AND t.`To` = c.Address
GROUP BY
	Symbol,
	Sender,
	TxDate
ORDER BY
	TxCount DESC
LIMIT 5000;
-- Equivalent query using tx_view_2
SELECT
	sum(t.Status) AS TxCount,
	sum(t.Status * t.Amount2 * power(10, -c.Decimals)) AS Amount,
	c.Symbol,
	t.Sender,
	t.BlockDate
FROM
	ethdb.tx_view_2 t
INNER JOIN ethdb.contracts c ON
	t.BlockDate >= '2022-01-01'
	AND t.BlockDate < '2022-01-02'
	AND t.Method = 'transfer'
	AND c.Symbol != ''
	AND t.Contract = c.Address
GROUP BY
	Symbol,
	Sender,
	BlockDate
ORDER BY
	TxCount DESC
LIMIT 5000;

-- Token transfer total by symbol, recipient and date
SELECT
	sum(t.Status) AS TxCount,
	sum(divide(t.Status * arrayElement(t.`Params.ValueDouble`, 2), exp10(c.Decimals))) AS Amount,
	c.Symbol,
	substring(arrayElement(t.`Params.ValueString`, 1), 3) AS Recipient,
	toDate(t.BlockTime) AS TxDate
FROM
	ethdb.transactions t
INNER JOIN ethdb.contracts c ON
	t.BlockTime >= '2022-01-01 00:00:00'
	AND t.BlockTime < '2022-01-02 00:00:00'
	AND t.Method = 'transfer'
	AND c.Symbol != ''
	AND t.`To` = c.Address
GROUP BY
	Symbol,
	Recipient,
	TxDate
ORDER BY
	TxCount DESC
LIMIT 5000;
-- Equivalent query using tx_view_2
SELECT
	sum(t.Status) AS TxCount,
	sum(t.Status * t.Amount2 * power(10, -c.Decimals)) AS Amount,
	c.Symbol,
	substring(t.Value1, 3) AS Recipient,
	t.BlockDate
FROM
	ethdb.tx_view_2 t
INNER JOIN ethdb.contracts c ON
	t.BlockDate >= '2022-01-01'
	AND t.BlockDate < '2022-01-02'
	AND t.Method = 'transfer'
	AND c.Symbol != ''
	AND t.Contract = c.Address
GROUP BY
	Symbol,
	Recipient,
	BlockDate
ORDER BY
	TxCount DESC
LIMIT 5000;

-- Test log views
SELECT * FROM ethdb.log_params_view;
SELECT * FROM ethdb.log_view;
SELECT * FROM ethdb.log_view_1;
SELECT * FROM ethdb.log_view_2;
SELECT * FROM ethdb.log_view_3;
SELECT * FROM ethdb.log_view_4;
SELECT * FROM ethdb.log_view_5;
