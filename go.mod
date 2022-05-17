module github.com/open-dovetail/eth-track

go 1.16

require (
	github.com/aws/aws-sdk-go-v2 v1.16.3
	github.com/aws/aws-sdk-go-v2/config v1.15.5
	github.com/aws/aws-sdk-go-v2/service/s3 v1.26.9
	github.com/aws/aws-sdk-go-v2/service/secretsmanager v1.15.7
	github.com/georgysavva/scany v0.3.0
	github.com/golang/glog v1.0.0
	github.com/gorilla/websocket v1.4.2 // indirect
	github.com/jackc/pgx/v4 v4.16.0
	github.com/mailru/go-clickhouse v1.7.0
	github.com/mitchellh/mapstructure v1.4.1 // indirect
	github.com/pkg/errors v0.9.1
	github.com/stretchr/testify v1.7.0
	github.com/umbracle/ethgo v0.1.1
	golang.org/x/net v0.0.0-20220127200216-cd36cc0744dd // indirect
	golang.org/x/sync v0.0.0-20190423024810-112230192c58
)

// replace github.com/open-dovetail/eth-track => /Users/yxu/work/open-dovetail/eth-track
