package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strings"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	// Import the generated protobuf package
	"google.golang.org/protobuf/proto"

	// NOTE: Adjust the path below if your generated file is elsewhere
	pb "warwickcodingsociety.com/producer/models/stockfeed"
)

// Global state for mean-reverting price generation
const (
	MEAN_PRICE     = 100.0
	REVERSION_RATE = 0.05
	VOLATILITY     = 5.0
)

var (
	// Command-line flags
	tickerName string
	rateMsgsPs int
	brokerAddr string
	formatType string // New flag for output format

	// State for the price generator
	currentPrice float64
)

var allowedTickers = []string{"STK_ONE", "STK_TWO"}
var allowedFormats = []string{"json", "protobuf"}

func init() {
	// Configure command-line flags (No defaults, set to zero values)
	flag.StringVar(&tickerName, "ticker", "", "MANDATORY: The name of the ticker (and Kafka topic) to produce to. Must be one of: STK_ONE, STK_TWO.")
	flag.IntVar(&rateMsgsPs, "rate", 0, "MANDATORY: The rate of data production in messages per second.")
	flag.StringVar(&brokerAddr, "broker", "", "MANDATORY: The Kafka broker address (e.g., my-kafka-service:9092).")
	flag.StringVar(&formatType, "format", "", "MANDATORY: The output format. Must be one of: 'json' or 'protobuf'.")

	// Initialize random seed and starting price...
	rand.Seed(time.Now().UnixNano())
	currentPrice = MEAN_PRICE
}

func generatePrice() float64 {
	diffFromMean := MEAN_PRICE - currentPrice
	meanReversionPull := diffFromMean * REVERSION_RATE
	randomStep := rand.NormFloat64() * VOLATILITY
	priceChange := meanReversionPull + randomStep

	currentPrice += priceChange
	if currentPrice <= 0 {
		currentPrice = 0.01
	}
	return currentPrice
}

// produceMessage creates the data and sends it to Kafka
func produceMessage(p *kafka.Producer, format string) {
	// Generate a new price and convert it to int32
	priceFloat := generatePrice()
	priceInt := int32(priceFloat * 100)

	// *** CRITICAL CHANGE: Get the timestamp in nanoseconds ***
	nanos := time.Now().UnixNano()

	// Create the Protobuf message instance
	messagePayload := &pb.StockUpdate{
		Timestamp: nanos, // Using nanoseconds for maximum precision (int64 is required)
		Price:     priceInt,
	}

	var value []byte
	var err error

	// --- Conditional Marshalling Logic ---
	if strings.ToLower(format) == "protobuf" {
		// Marshal to Protobuf Binary
		value, err = proto.Marshal(messagePayload)
		if err != nil {
			log.Printf("Failed to marshal Protobuf: %v", err)
			return
		}
		log.Printf("Produced Protobuf message to topic %s: Price: %.2f (Nanos: %d)",
			tickerName, priceFloat, nanos)

	} else { // JSON format
		// Use a temporary struct for JSON output, ensuring it also uses nanoseconds
		jsonStruct := struct {
			Ticker    string  `json:"ticker"`
			Timestamp int64   `json:"timestamp"` // int64 can hold nanoseconds
			Price     float64 `json:"price"`
		}{
			Ticker:    tickerName,
			Timestamp: nanos, // *** USING NANOSECONDS FOR JSON TOO ***
			Price:     priceFloat,
		}

		// Marshal to JSON
		value, err = json.Marshal(jsonStruct)
		if err != nil {
			log.Printf("Failed to marshal JSON: %v", err)
			return
		}
		log.Printf("Produced JSON message to topic %s: Price: %.2f (Nanos: %d)",
			tickerName, priceFloat, nanos)
	}

	// Check for marshalling error
	if err != nil {
		return
	}

	// Produce the message to the topic
	err = p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &tickerName, Partition: kafka.PartitionAny},
		Value:          value,
		Key:            []byte(tickerName),
	}, nil)

	if err != nil {
		log.Printf("Failed to produce message: %v", err)
	}
}

// deliveryReportHandler asynchronously handles delivery reports from Kafka
func deliveryReportHandler(p *kafka.Producer) {
	for e := range p.Events() {
		switch ev := e.(type) {
		case *kafka.Message:
			if ev.TopicPartition.Error != nil {
				log.Printf("Delivery failed: %v", ev.TopicPartition.Error)
			}
		}
	}
}

func main() {
	flag.Parse()

	isValidTicker := false
	for _, allowed := range allowedTickers {
		if tickerName == allowed {
			isValidTicker = true
			break
		}
	}

	isValidFormat := false
	for _, allowed := range allowedFormats {
		if formatType == allowed {
			isValidFormat = true
			break
		}
	}

	if !isValidTicker {
		allowedList := strings.Join(allowedTickers, ", ")
		fmt.Printf("❌ Error: Invalid ticker name '%s'. Allowed tickers are: %s\n", tickerName, allowedList)
		flag.Usage() // Print usage help
		os.Exit(1)
	}

	if !isValidFormat {
		allowedList := strings.Join(allowedFormats, ", ")
		fmt.Printf("❌ Error: Invalid format name '%s'. Allowed formats are: %s\n", formatType, allowedList)
		flag.Usage() // Print usage help
		os.Exit(1)
	}

	log.Printf("🚀 Starting Kafka Producer")
	log.Printf("   -> Ticker/Topic: %s", tickerName)
	log.Printf("   -> Broker Address: %s", brokerAddr)
	log.Printf("   -> Production Rate: %d msg/sec", rateMsgsPs)
	log.Printf("   -> Output Format: %s", strings.ToUpper(formatType)) // Highlight the chosen format

	// --- Create the Kafka Producer ---
	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": brokerAddr})
	if err != nil {
		fmt.Printf("Failed to create producer: %s\n", err)
		os.Exit(1)
	}
	defer p.Close()

	// --- Start Delivery Report Handler in a Goroutine ---
	go deliveryReportHandler(p)

	// --- Start the Ticker and Production Loop ---
	interval := time.Duration(1000/rateMsgsPs) * time.Millisecond
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	log.Printf("Starting production loop (Interval: %v)... Press Ctrl+C to stop.", interval)

	for {
		select {
		case <-ticker.C:
			// Pass the format to the production function
			produceMessage(p, formatType)
		case e := <-p.Events():
			_ = e
		}
	}
}
