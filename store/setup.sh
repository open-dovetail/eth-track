WORK=~/work/clickhouse
cp tables.sql $WORK/ethdb.sql
cd $WORK
./clickhouse client --password clickhouse --multiquery < ethdb.sql
