package integration

import (
	"os/exec"
	"strings"

	ic "github.com/patricioibar/distribuidos-tp/innercommunication"
	mw "github.com/patricioibar/distribuidos-tp/middleware"
)

func SimulateProcessing(
	testName string,
	rabbitAddr string,
	input []ic.RowsBatch,
	expectedOutputBatches int,
) []string {
	conn := mw.GetConnection(rabbitAddr)
	defer conn.Close()

	sourceName := "input_" + testName
	producer, _ := mw.NewProducer(sourceName, rabbitAddr)
	for _, batch := range input {
		data, _ := batch.Marshal()
		producer.Send(data)
	}

	outputName := "output_" + testName
	consumer, _ := mw.NewConsumer("test_consumer_"+testName, outputName, rabbitAddr)

	var result []string
	msgReceived := make(chan struct{}, expectedOutputBatches)
	callback := func(consumeChannel mw.MiddlewareMessage, done chan *mw.MessageMiddlewareError) {
		println("#### Received message:", string(consumeChannel.Body))
		result = append(result, string(consumeChannel.Body))
		done <- nil
		msgReceived <- struct{}{}
	}
	consumer.StartConsuming(callback)
	defer consumer.Close()
	for i := 0; i < expectedOutputBatches; i++ {
		<-msgReceived
	}
	return result
}

func RunBackgroundCmd(cmdStr string) chan error {
	cmdArr := strings.Split(cmdStr, " ")
	proc := exec.Command(cmdArr[0], cmdArr[1:]...)
	dockerErr := make(chan error)
	go func() {
		dockerErr <- proc.Run()
	}()
	return dockerErr
}
