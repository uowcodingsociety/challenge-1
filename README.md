# challenge materials

you need to have [protobuf](https://grpc.io/docs/protoc-installation/), [protobuf for go](https://grpc.io/docs/languages/go/quickstart/), and [golang](https://go.dev/doc/install) installed for this challenge.

Compile with the build script in `./build.sh`.

# Kafka Stock Producer — Architecture, Function, and Usage

## Architecture

- **Main Components**
  - `main()`: Parses CLI flags, validates input, initializes Kafka producer, starts production loop.
  - `produceMessage()`: Generates a mean-reverting stock price, serializes it (JSON or Protobuf), and sends it to Kafka.
  - `generatePrice()`: Implements mean-reverting stochastic price evolution.
  - `deliveryReportHandler()`: Async goroutine that logs Kafka delivery events.
- **Data Models**
  - JSON: Inline struct with `ticker`, `timestamp` (ns), `price`.
  - Protobuf: `pb.StockUpdate` containing `timestamp` (int64, ns) and `price` (int32, cents).

## Function

- Produces synthetic stock feed data for tickers `STK_ONE` or `STK_TWO`.
- Uses a **mean-reversion model**:  
  `price += (mean - price)*rate + random_normal*volatility`
- Sends messages to Kafka at a fixed rate (`--rate`) using Confluent Kafka Go client.
- Supports **JSON** or **Protobuf** serialization via the `--format` flag.
- Timestamps always encoded as **Unix nanoseconds**.

## Usage

### Run

```
./stock-producer
    --ticker STK_ONE
    --rate 10
    --broker my-kafka:9092
    --format protobuf
```

### Flags

- `--ticker` (required): `STK_ONE` or `STK_TWO`.
- `--rate` (required): Messages per second.
- `--broker` (required): Kafka bootstrap server.
- `--format` (required): `json` or `protobuf`.

### Notes

- Produces indefinitely until killed (Ctrl+C).
- Logs each produced message with price + timestamp.
- Protobuf messages require the generated `.pb.go` file in `pb`.

### Protobuf (`pb.StockUpdate`)

| Field       | Type    | Meaning                                     |
| ----------- | ------- | ------------------------------------------- |
| `timestamp` | `int64` | Unix time in **nanoseconds**                |
| `price`     | `int32` | Price in **cents** (float converted to int) |

### JSON Output

JSON uses an inline struct with the following fields:

| Field       | Type      | Meaning                      |
| ----------- | --------- | ---------------------------- |
| `ticker`    | `string`  | The stock symbol/topic       |
| `timestamp` | `int64`   | Unix time in **nanoseconds** |
| `price`     | `float64` | Price as floating-point      |

**Example JSON**

```json
{
  "ticker": "STK_ONE",
  "timestamp": 1732030100456123456,
  "price": 123.45
}
```
