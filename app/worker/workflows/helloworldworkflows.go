// app/worker/workflows/helloworldworkflows.go
package workflows

import (
	"context"
	"errors"

	"fmt"
	"time"

	"github.com/nndd91/cadence-api-example/app/Queue"

	"go.uber.org/cadence/activity"
	"go.uber.org/cadence/workflow"
	"go.uber.org/zap"
)

var Response_Queue1 = Queue.Queue2{}
var Response_Queue2 = Queue.Queue2{}
var Response_Queue3 = Queue.Queue2{}

var s = 0

// Initializing the task Queue for Process 1, 2 and 3
var Q1 = Queue.Queue{

	Size: 10,
}

var Q2 = Queue.Queue{

	Size: 10,
}
var Q3 = Queue.Queue{

	Size: 10,
}

func init() {
	// Registering workflow and activtiy
	workflow.Register(customerWorkflow)
	activity.Register(Activity1)
	activity.Register(Activity3)
	activity.Register(Activity2)

}

const SignalName = "taskCompleted"

type TaskCompletedSignal struct {
	WorkflowID string
	TaskID     int
}

// Task_List Name
const TaskListName = "Service_process"

func customerWorkflow(ctx workflow.Context, id int) error {
	ao := workflow.ActivityOptions{
		ScheduleToStartTimeout: time.Minute * 60,
		StartToCloseTimeout:    time.Minute * 60,
		HeartbeatTimeout:       time.Minute * 60,
	}
	ctx = workflow.WithActivityOptions(ctx, ao)

	// To get the Workflow id
	wid := workflow.GetInfo(ctx).WorkflowExecution.ID

	logger := workflow.GetLogger(ctx)
	logger.Info("Customer workflow started")
	var Result string

	// Start activities
	err := workflow.ExecuteActivity(ctx, Activity1, wid, id).Get(ctx, &Result)
	if err != nil {
		logger.Error("Activity failed.", zap.Error(err))
		return err
	}

	err2 := workflow.ExecuteActivity(ctx, Activity2, id).Get(ctx, &Result)
	if err2 != nil {
		logger.Error("Activity failed.", zap.Error(err2))
		return err2
	}

	err3 := workflow.ExecuteActivity(ctx, Activity3, wid, id).Get(ctx, &Result)
	if err3 != nil {
		logger.Error("Activity failed.", zap.Error(err3))
		return err3
	}

	logger.Info("Workflow completed.", zap.String("Result", Result))

	return nil
}
func signalHandler(ctx workflow.Context) error {
	// Listen for signals
	signalChan := workflow.GetSignalChannel(ctx, SignalName)

	for {
		var signal TaskCompletedSignal
		// Wait for signal
		if !signalChan.Receive(ctx, &signal) {
			// Channel closed
			return nil
		}

		// Handle the received signal
		// Here you can implement logic to process the signal and update workflow state
		logger := workflow.GetLogger(ctx)
		logger.Info("Received signal", zap.Any("signal", signal))
	}

	return nil
}

func Activity1(ctx context.Context, workflow_id string, id int) (string, error) {
	//Enququeing in task queue Q1,Q2,Q3 Based on ID-service dependent
	logger := activity.GetLogger(ctx)
	logger.Info("Activty 1 started")

	switch id {
	case 1, 4:
		ans, err := activtiy1_fn(workflow_id, id, &Q1)
		return ans, err
	case 2, 5:
		ans, err := activtiy1_fn(workflow_id, id, &Q2)
		return ans, err
	case 3, 6:
		ans, err := activtiy1_fn(workflow_id, id, &Q3)
		return ans, err
	}

	return "Completed", nil
}

func activtiy1_fn(workflow_id string, id int, q *Queue.Queue) (string, error) {

	for q.GetLength() >= q.Size/2 {
		time.Sleep(time.Millisecond)
	}

	customer1 := Queue.New(workflow_id, id)
	ans := fmt.Sprintf("Enqueud at %s", time.Now())
	_, err := q.Enqueue(customer1)
	if err != nil {
		panic(err)
	}
	return ans, err
}

func Activity2(ctx context.Context, id int) (string, error) {
	// To sort the Queue based on priority algorithm
	logger := activity.GetLogger(ctx)
	logger.Info("Activty 2 started")

	switch id {
	case 1, 4:
		Q1.SortCustomers()
		//Q1.Display()
		return "Queue 1 sorted", nil
	case 2, 5:
		Q2.SortCustomers()
		//Q2.Display()
		return "Queue 2 sorted", nil
	case 3, 6:
		Q3.SortCustomers()
		//Q3.Display()
		return "Queue 3 sorted", nil
	}

	return "Activity 2 Completed", nil
}

func Activity3(ctx context.Context, wid string, id int) (string, error) {
	// Waiting for signal to get complete
	logger := activity.GetLogger(ctx)
	logger.Info("Activty 2 started")

	switch id {
	case 1, 4:
		ans, err := Activity3_fn(wid, &Response_Queue1)
		return ans, err
	case 2, 5:
		ans, err := Activity3_fn(wid, &Response_Queue2)
		return ans, err
	case 3, 6:
		ans, err := Activity3_fn(wid, &Response_Queue3)
		return ans, err
	}

	return "Activity 3 Completed", nil
}

func Activity3_fn(wid string, q2 *Queue.Queue2) (string, error) {

	time.Sleep(time.Second * 9)
	for s > -1 {

		response := q2.SearchAndRemove(wid)

		if response == true {
			return "Task Completed", nil
		}

		time.Sleep(time.Millisecond)
	}
	return "Cant complete", errors.New("error")

}
func init() {
	// Registering workflow, activity, and signal handler
	workflow.Register(customerWorkflow)
	workflow.Register(signalHandler)
	activity.Register(Activity1)
	activity.Register(Activity3)
	activity.Register(Activity2)
}
