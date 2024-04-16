// app/worker/main.go
package main

import (
	"fmt"
	"time"

	"github.com/nndd91/cadence-api-example/app/adapters/cadenceAdapter"
	"github.com/nndd91/cadence-api-example/app/config"
	"github.com/nndd91/cadence-api-example/app/worker/workflows"
	"go.uber.org/cadence/worker"
	"go.uber.org/zap"
)

var task_counter1 = 0
var task_counter2 = 0
var task_counter3 = 0
var c = 0
var a = 0
var b = 0

func startWorkers(h *cadenceAdapter.CadenceAdapter, taskList string) {
	// Configure worker options.
	workerOptions := worker.Options{
		MetricsScope: h.Scope,
		Logger:       h.Logger,
	}

	cadenceWorker := worker.New(h.ServiceClient, h.Config.Domain, taskList, workerOptions)
	err := cadenceWorker.Start()
	if err != nil {
		h.Logger.Error("Failed to start workers.", zap.Error(err))
		panic("Failed to start workers")
	}
}
func main() {

	fmt.Println("Starting Worker..")
	var appConfig config.AppConfig
	appConfig.Setup()
	var cadenceClient cadenceAdapter.CadenceAdapter
	cadenceClient.Setup(&appConfig.Cadence)

	startWorkers(&cadenceClient, workflows.TaskListName)
	go worker1()
	go worker2()
	go worker3()

	//time.Sleep(time.Second * 10)
	//go start()

	fmt.Println("All workers are readyy ")
	// The workers are supposed to beS long running process that should not exit.

	select {}
}

/*
func start() {

		for i := 1; i <= numWorkflows; i++ {
			// Generate a random workflow input
			randomNumber := rand.Intn(6) + 1
			fmt.Println(randomNumber)
			dockerCmd := fmt.Sprintf("docker run --rm %s --address %s -do %s workflow start --et 1000 --tl %s --wt %s --input %d",
				cadenceCLIImage, cadenceAddress, domain, taskList, workflowType, randomNumber)
			executeCommand(dockerCmd)

			// Execute Docker command
			//	fmt.Println("Executing Docker command:", dockerCmd)

		}
	}

	func executeCommand(command string) {
		cmd := exec.Command("cmd", "/c", command)
		cmd.Stdout = os.Stdout

		//cmd.Stderr = os.Stderr

		err := cmd.Run()
		if err != nil {

			//fmt.Printf("Error executing Docker command: %v\n", err)
			os.Exit(1)
		}

}
*/
func worker2() {

	for a > -1 {
		if task_counter2 < 5 {

			task_counter2++
			go testing2()
		} else {
			time.Sleep(time.Second)
		}

	}

}

func worker3() {

	for b > -1 {
		if task_counter3 < 5 {
			task_counter3++
			go testing3()
		} else {
			time.Sleep(time.Second)
		}

	}

}
func worker1() {

	for c > -1 {
		if task_counter1 < 5 {
			task_counter1++
			go testing1()
		} else {
			time.Sleep(time.Second)
		}

	}
}

func testing2() {

	response2, err2 := workflows.Q2.Dequeue()

	if err2 == nil {

		time.Sleep(time.Second * 10)

		workflows.Response_Queue2.Enqueue2(response2)

		fmt.Println("signal sent to Response queue 2")
		time.Sleep(time.Millisecond)

	}
	task_counter2--
}

func testing3() {

	response3, err3 := workflows.Q3.Dequeue()

	if err3 == nil {

		time.Sleep(time.Second * 10)

		workflows.Response_Queue3.Enqueue2(response3)

		fmt.Println("signal sent to Response queue 3")
		time.Sleep(time.Millisecond)

	}
	task_counter3--
}

func testing1() {

	response1, err1 := workflows.Q1.Dequeue()

	if err1 == nil {

		time.Sleep(time.Second * 10)

		workflows.Response_Queue1.Enqueue2(response1)

		fmt.Println("signal sent to Response queue 1")
		time.Sleep(time.Millisecond)

	}
	task_counter1--
}
