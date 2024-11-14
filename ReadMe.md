
# Real-Time Data Streaming API with Golang and Redpanda (Kafka)

My name is Deeptanshu Sankhwar, and this is my submission to the problem statement to build a high performance API in Golang that streams data to and from Redpanda (Kafka)

## Steps to setup Redpanda (Kafka)
Go over the yaml file for dockerizing a Redpanda (kafka) instance in the project as docker-compose.yml, which is externally running at port 19092 and internally at the port 9092.

The steps to spin up this instance are as follows
```
docker-compose up
```

Once we do that, our container for Redpanda shall be ready and we can navigate to localhost:8080 in the browser to visit the Redpanda console over a GUI.

Additionally we can run the following command to check the brokers, topics and partitions within the deployed Redpanda instance 
```
kcat -L -b localhost:19092
```

## Steps to run the project
After cloning, you may wanna a .env file which has the following varible
```
API_KEY = blockhouse
```
For my case, I have chosen the correct API key string to authenticate the stream APIs to be "blockhouse"

The packages we'd need to install are

```
go get github.com/IBM/sarama
go get github.com/joho/godotenv
go get github.com/gin-gonic/gin
go get github.com/gorilla/websocket
go get go.uber.org/zap
go get github.com/stretchr/testify
```

Then finally to run:

```
go run main.go
```

## Understading the benchmarks
In the codebase, benchmark/benchmark.go contains the script to subject the streaming API to handle 1000 concurent streams to stream 100,000 messages. I have given the performance metrics to measure throughput, latency and resource utilization, which came to be:
```
Benchmarking Results:
Total Messages Sent: 99888
Time Elapsed: 7.56 seconds
Throughput: 13209.01 messages/second
Average Latency per message: 0.08 ms
```

## Testing
There are a few unit test cases to test producers and consumers and one integration test to simulate the APIs, which can be run as:
```
go test ./...
```

### Remarks
For any concerns, please reach out to me at desa4692@colorado.edu