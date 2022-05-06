DROP TABLE IF EXISTS eth.contracts;
CREATE TABLE eth.contracts
(
    Address CHAR(40) primary key,
    Name VARCHAR(40),
    Symbol VARCHAR(40),
    Decimals BIGINT,
    TotalSupply FLOAT8,
    LastEventDate DATE,
    LastErrorDate DATE,
    ABI VARCHAR(32768)
);

DROP TABLE IF EXISTS eth.blocks;
CREATE TABLE eth.blocks
(
    Hash CHAR(64) primary key,
    Number BIGINT sortkey,
    ParentHash CHAR(64),
    Miner CHAR(40),
    Difficulty FLOAT8,
    GasLimit BIGINT,
    GasUsed BIGINT,
    BlockTime TIMESTAMP
);

DROP TABLE IF EXISTS eth.transactions;
CREATE TABLE eth.transactions
(
    Hash CHAR(64) primary key,
    BlockNumber BIGINT not null,
    TxnIndex BIGINT not null,
    FromAddress CHAR(40),
    ToAddress CHAR(40),
    Input VARBYTE,
    Method VARCHAR(40),
    ArgsLen INTEGER,
    Arg_1 VARCHAR(40),
    S_Value_1 VARCHAR(4096),
    F_Value_1 FLOAT8,
    Arg_2 VARCHAR(40),
    S_Value_2 VARCHAR(4096),
    F_Value_2 FLOAT8,
    Arg_3 VARCHAR(40),
    S_Value_3 VARCHAR(4096),
    F_Value_3 FLOAT8,
    Arg_4 VARCHAR(40),
    S_Value_4 VARCHAR(4096),
    F_Value_4 FLOAT8,
    Arg_5 VARCHAR(40),
    S_Value_5 VARCHAR(4096),
    F_Value_5 FLOAT8,
    GasPrice BIGINT,
    Gas BIGINT,
    Value FLOAT8,
    Nonce BIGINT,
    BlockTime TIMESTAMP sortkey
);

DROP TABLE IF EXISTS eth.logs;
CREATE TABLE eth.logs
(
    BlockNumber BIGINT,
    LogIndex BIGINT,
    TxnIndex BIGINT,
    TxnHash CHAR(64),
    Address CHAR(40),
    Data VARBYTE,
    Event VARCHAR(40),
    ArgsLen INTEGER,
    Arg_1 VARCHAR(40),
    S_Value_1 VARCHAR(4096),
    F_Value_1 FLOAT8,
    Arg_2 VARCHAR(40),
    S_Value_2 VARCHAR(4096),
    F_Value_2 FLOAT8,
    Arg_3 VARCHAR(40),
    S_Value_3 VARCHAR(4096),
    F_Value_3 FLOAT8,
    Arg_4 VARCHAR(40),
    S_Value_4 VARCHAR(4096),
    F_Value_4 FLOAT8,
    Arg_5 VARCHAR(40),
    S_Value_5 VARCHAR(4096),
    F_Value_5 FLOAT8,
    BlockTime TIMESTAMP sortkey,
    primary key(BlockNumber, LogIndex)
);

DROP TABLE IF EXISTS eth.progress;
CREATE TABLE eth.progress
(
    ProcessID INTEGER primary key,
    HiBlock BIGINT,
    LowBlock BIGINT
);
insert into eth.progress values (1, 0, 0);