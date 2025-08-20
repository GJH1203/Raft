package mapreduce

import (
	"fmt"
	"sync"
)

// schedule starts and waits for all tasks in the given phase (Map or Reduce).
func (mr *Master) schedule(phase jobPhase) {
	var ntasks int
	var nios int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mr.files)
		nios = mr.nReduce
	case reducePhase:
		ntasks = mr.nReduce
		nios = len(mr.files)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nios)

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
	//
	
	// Use a WaitGroup to wait for all tasks to complete
	var wg sync.WaitGroup
	wg.Add(ntasks)
	
	// Create a channel for task assignment
	taskChan := make(chan int, ntasks)
	
	// Queue all tasks
	for i := 0; i < ntasks; i++ {
		taskChan <- i
	}
	
	// Start goroutines to process tasks
	go func() {
		for taskNumber := range taskChan {
			// Wait for an available worker
			worker := <-mr.registerChannel
			
			// Assign task to worker in a new goroutine
			go func(w string, task int) {
				// Prepare RPC arguments
				var args DoTaskArgs
				args.JobName = mr.jobName
				args.Phase = phase
				args.TaskNumber = task
				args.NumOtherPhase = nios
				
				// Set the file for map tasks
				if phase == mapPhase {
					args.File = mr.files[task]
				}
				
				// Call the worker
				ok := call(w, "Worker.DoTask", &args, new(struct{}))
				
				if ok {
					// Task completed successfully
					wg.Done()
					// Return worker to the pool
					mr.registerChannel <- w
				} else {
					// Worker failed, retry the task
					// This handles Part IV - worker failures
					taskChan <- task
				}
			}(worker, taskNumber)
		}
	}()
	
	// Wait for all tasks to complete
	wg.Wait()
	
	// Close the task channel since all tasks are done
	close(taskChan)
	
	fmt.Printf("Schedule: %v phase done\n", phase)
}
