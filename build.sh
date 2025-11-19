protoc \
  --go_out=. \
  --go_opt=paths=source_relative \
  models/stockfeed.proto

go build -trimpath -ldflags="-s -w" -o bin/appname .
