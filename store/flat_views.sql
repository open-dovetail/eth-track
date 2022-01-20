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
		BlockTime,
		toDate(BlockTime) AS BlockDate,
		toStartOfMonth(BlockTime) AS BlockMonth,
		toYear(BlockTime) AS BlockYear
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
		BlockTime,
		toDate(BlockTime) AS BlockDate,
		toStartOfMonth(BlockTime) AS BlockMonth,
		toYear(BlockTime) AS BlockYear
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
		BlockTime,
		toDate(BlockTime) AS BlockDate,
		toStartOfMonth(BlockTime) AS BlockMonth,
		toYear(BlockTime) AS BlockYear
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
		BlockTime,
		toDate(BlockTime) AS BlockDate,
		toStartOfMonth(BlockTime) AS BlockMonth,
		toYear(BlockTime) AS BlockYear
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
		BlockTime,
		toDate(BlockTime) AS BlockDate,
		toStartOfMonth(BlockTime) AS BlockMonth,
		toYear(BlockTime) AS BlockYear
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
		BlockTime,
		toDate(BlockTime) AS BlockDate,
		toStartOfMonth(BlockTime) AS BlockMonth,
		toYear(BlockTime) AS BlockYear
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
		BlockTime,
		toDate(BlockTime) AS BlockDate,
		toStartOfMonth(BlockTime) AS BlockMonth,
		toYear(BlockTime) AS BlockYear
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
		BlockTime,
		toDate(BlockTime) AS BlockDate,
		toStartOfMonth(BlockTime) AS BlockMonth,
		toYear(BlockTime) AS BlockYear
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
		BlockTime,
		toDate(BlockTime) AS BlockDate,
		toStartOfMonth(BlockTime) AS BlockMonth,
		toYear(BlockTime) AS BlockYear
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
		BlockTime,
		toDate(BlockTime) AS BlockDate,
		toStartOfMonth(BlockTime) AS BlockMonth,
		toYear(BlockTime) AS BlockYear
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
		BlockTime,
		toDate(BlockTime) AS BlockDate,
		toStartOfMonth(BlockTime) AS BlockMonth,
		toYear(BlockTime) AS BlockYear
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
		BlockTime,
		toDate(BlockTime) AS BlockDate,
		toStartOfMonth(BlockTime) AS BlockMonth,
		toYear(BlockTime) AS BlockYear
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
		BlockTime,
		toDate(BlockTime) AS BlockDate,
		toStartOfMonth(BlockTime) AS BlockMonth,
		toYear(BlockTime) AS BlockYear
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
		BlockTime,
		toDate(BlockTime) AS BlockDate,
		toStartOfMonth(BlockTime) AS BlockMonth,
		toYear(BlockTime) AS BlockYear
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
		BlockTime,
		toDate(BlockTime) AS BlockDate,
		toStartOfMonth(BlockTime) AS BlockMonth,
		toYear(BlockTime) AS BlockYear
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
		BlockTime,
		toDate(BlockTime) AS BlockDate,
		toStartOfMonth(BlockTime) AS BlockMonth,
		toYear(BlockTime) AS BlockYear
	FROM
		ethdb.logs 
    ARRAY JOIN Params
	WHERE
		Event != 'UNKNOWN'
		AND (Params.ValueString != '' OR Params.ValueDouble > 0);

DROP VIEW IF EXISTS ethdb.block_view;
CREATE VIEW ethdb.block_view AS
    SELECT
		Hash,
		`Number` AS BlockNumber,
		ParentHash,
		Miner,
		Difficulty,
		GasLimit,
		GasUsed,
		Status,
		BlockTime,
		toDate(BlockTime) AS BlockDate,
		toStartOfMonth(BlockTime) AS BlockMonth,
		toYear(BlockTime) AS BlockYear
	FROM
		ethdb.blocks;

DROP VIEW IF EXISTS ethdb.contract_view;
CREATE VIEW ethdb.contract_view AS
    SELECT
		Address,
		Name,
		Symbol,
		Decimals,
		TotalSupply,
		UpdatedDate,
		StartEventDate,
		LastEventDate,
		LastErrorTime,
		ABI,
		toStartOfMonth(LastEventDate) AS LastEventMonth,
		toYear(LastEventDate) AS LastEventYear
	FROM
		ethdb.contracts;
