WORK=~/work/clickhouse
cp tables.sql $WORK/ethdb.sql
cp aggregated_views.sql $WORK/aggregated_views.sql

cd $WORK
./clickhouse client --password clickhouse --multiquery < ethdb.sql
./clickhouse client --password clickhouse --multiquery < aggregated_views.sql
