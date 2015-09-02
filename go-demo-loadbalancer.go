/******************************************************************************
* 
* Go Demo: implementation of a simple (local) load balancer using channels
*
*
* this is the implementation of demo presented in the following video:
*   http://blog.golang.org/go-programming-session-video-from
*
******************************************************************************/

package main

import (
	"container/heap"
	"fmt"
	"math/rand"
	"time"
)

const (
	// number of workers and requesters
	nWorkers    = 10
	nRequesters = 100

	// buffer length of different channels
	requestsLength = nRequesters // if all work would go to a single worker
	doneLength     = nWorkers    // if all workers are done at once
	workLength     = nRequesters // if all requesters schedule a job at once
)

var (
	// scalings for time.Sleep()
	scaleSleepDurationRequest = rand.Float64() * 2.0
	scaleSleepDurationWork    = 2 * scaleSleepDurationRequest * nWorkers / nRequesters
)

// a simple job type consisting of a function to execute and a channel to
// report the result of the function back
type Request struct {
	// the workload
	fn func() int
	// response channel from worker => requester
	response chan int
}

// helper function to scale one second by given float
func sleepDuration(scale float64) time.Duration {
	return time.Duration(float64(time.Second) * scale)
}

// Job to perform (just a random sleep yet)
func workFn() int {
	time.Sleep(sleepDuration(scaleSleepDurationWork)) // spend time
	return 1
}

// simple job requester: request ONE job at a time and waits for the result
func requester(work chan Request) {
	// the response channel from workers => to myself
	c := make(chan int)

	for {
		work <- Request{workFn, c}                           // send request
		_ = <-c                                              // read response
		time.Sleep(sleepDuration(scaleSleepDurationRequest)) // spend time
	}
}

// Worker type
type Worker struct {
	requests chan Request // channel of pending jobs
	pending  int          // pending count
	index    int          // required for interface Heap implementation (sort)
}

// simple worker: fetches job, sends result to requester and reports back to
// loadbalancer
func (w *Worker) work(done chan *Worker) {
	for {
		req := <-w.requests      // fetch next request
		req.response <- req.fn() // calculate and return answer to requester
		done <- w                // tell balancer we are done
	}
}

// pool of workers implementing Heap interface so it is kept sorted
type Pool []*Worker

// implementation for interface Heap
func (p Pool) Less(i, j int) bool {
	return p[i].pending < p[j].pending
}

// implementation for interface Heap
func (p Pool) Len() int {
	return len(p)
}

// Swap changes position of two Pool entries.
// implementation for interface Heap
func (p Pool) Swap(i, j int) {
	// swap entries
	p[i], p[j] = p[j], p[i]
	// refresh indices
	p[i].index = i
	p[j].index = j
}

// Push adds element to end of Pool
// implementation for interface Heap
func (p *Pool) Push(x interface{}) {
	n := len(*p)
	w := x.(*Worker)
	w.index = n // treat index correctly
	*p = append(*p, w)
}

// Pop removes last element from Pool and returns it
// implementation for interface Heap
func (p *Pool) Pop() interface{} {
	old := *p
	n := len(old)
	x := old[n-1]
	x.index = -1 // for safety
	*p = old[0 : n-1]
	return x
}

// load balancer with a pool of workers
type Balancer struct {
	pool      Pool         // pool of workers
	done      chan *Worker // back channel from workers for job completed handling
	doneCount int          // how many jobs completed
	openCount int          // how many jobs open
}

// returns new load balancer with given number of workers
func NewBalancer(nWorkers int) (b *Balancer) {
	b = new(Balancer)

	b.done = make(chan *Worker, doneLength)

	for i := 0; i < nWorkers; i++ {
		b.generateWorker()
	}

	return b
}

// print statistics about pending jobs per worker and open/completed jobs
func (b *Balancer) printStats() {
	for i := 0; i < len(b.pool); i++ {
		fmt.Printf("%3d ", b.pool[i].pending)
	}
	fmt.Printf("  open request %4d   completed %d", b.openCount, b.doneCount)
	fmt.Println("")
}

// generates a new Worker
func (b *Balancer) generateWorker() {
	w := new(Worker)
	w.requests = make(chan Request, requestsLength)
	heap.Push(&b.pool, w)
	go w.work(b.done)
}

// dispatch a new job request
func (b *Balancer) dispatch(req Request, silent bool) {
	w := heap.Pop(&b.pool).(*Worker)
	w.requests <- req
	w.pending++
	heap.Push(&b.pool, w)
	b.openCount++
	if !silent {
		b.printStats()
	}
}

// process "job done" event
func (b *Balancer) completed(w *Worker, silent bool) {
	b.doneCount++
	b.openCount--
	w.pending--
	heap.Fix(&b.pool, w.index)
	if !silent {
		b.printStats()
	}
}

// "main" loop of load balancer
func (b *Balancer) Balance(workToDo chan Request, silent bool) {
	for {
		select {
		case req := <-workToDo:
			b.dispatch(req, silent) // schedule job
		case workerDone := <-b.done:
			b.completed(workerDone, silent) // process "job done"
		}
	}
}

func main() {
	// generate channel for jobs
	jobsChannel := make(chan Request, workLength)

	// start job requesters
	for i := 0; i < nRequesters; i++ {
		go requester(jobsChannel)
	}

	// generate load balancer with given number of workers
	jobScheduler := NewBalancer(nWorkers)

	// start loadbalancer
	go jobScheduler.Balance(jobsChannel, false)

	// sleep forever (waking up now and then - doesn't matter)
	for {
		time.Sleep(10 * time.Second)
	}
}
