# eth-track

Decode Ethereum transactions and events, and store the result in a [ClickHouse](https://clickhouse.com/docs/en/) database for data analysis.

## Sample Query

ERC20 token transfer transactions with known symbols

```sql
SELECT 
    Hash, From, To, 
    arrayElement(Params.ValueString, 1) as Recipient, 
    divide(arrayElement(Params.ValueDouble, 2), exp10(Decimals)) as Amount, 
    BlockTime, Symbol, Decimals 
FROM ethdb.transactions t 
INNER JOIN ethdb.contracts c 
ON t.Method = 'transfer' AND c.Symbol != '' AND t.To = c.Address
```

Top daily transfers of ERC20 tokens

```sql
SELECT 
    count() as Count, 
    sum(divide(arrayElement(Params.ValueDouble, 2), exp10(Decimals))) as Amount, 
    Symbol, toDate(BlockTime) as Date 
FROM ethdb.transactions t 
INNER JOIN ethdb.contracts c 
ON t.Method = 'transfer' AND c.Symbol != '' AND t.To = c.Address 
GROUP BY Symbol, Date 
ORDER BY Count DESC
```