go build -buildmode=plugin ../mrapps/wc.go
go run mrworker.go wc.so
