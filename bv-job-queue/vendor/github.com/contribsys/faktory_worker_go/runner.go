package faktory_worker

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"

	faktory "github.com/contribsys/faktory/client"
)

type eventType int

const (
	Startup  eventType = 1
	Quiet    eventType = 2
	Shutdown eventType = 3
)

// Register registers a handler for the given jobtype.  It is expected that all jobtypes
// are registered upon process startup.
//
// faktory_worker.Register("ImportantJob", ImportantFunc)
func (mgr *Manager) Register(name string, fn Perform) {
	mgr.jobHandlers[name] = func(ctx Context, job *faktory.Job) error {
		return fn(ctx, job.Args...)
	}
}

// Manager coordinates the processes for the worker.  It is responsible for
// starting and stopping goroutines to perform work at the desired concurrency level
type Manager struct {
	Concurrency int
	Pool
	Logger Logger

	queues     []string
	middleware []MiddlewareFunc
	quiet      bool
	// The done channel will always block unless
	// the system is shutting down.
	done           chan interface{}
	shutdownWaiter *sync.WaitGroup
	jobHandlers    map[string]Handler
	eventHandlers  map[eventType][]func()

	// This only needs to be computed once. Store it here to keep things fast.
	weightedPriorityQueuesEnabled bool
	weightedQueues                []string
}

// Register a callback to be fired when a process lifecycle event occurs.
// These are useful for hooking into process startup or shutdown.
func (mgr *Manager) On(event eventType, fn func()) {
	mgr.eventHandlers[event] = append(mgr.eventHandlers[event], fn)
}

// After calling Quiet(), no more jobs will be pulled
// from Faktory by this process.
func (mgr *Manager) Quiet() {
	mgr.Logger.Info("Quieting...")
	mgr.quiet = true
	mgr.fireEvent(Quiet)
}

// Terminate signals that the various components should shutdown.
// Blocks on the shutdownWaiter until all components have finished.
func (mgr *Manager) Terminate() {
	mgr.Logger.Info("Shutting down...")
	close(mgr.done)
	mgr.fireEvent(Shutdown)
	mgr.shutdownWaiter.Wait()
	mgr.Pool.Close()
	mgr.Logger.Info("Goodbye")
	os.Exit(0)
}

// Use adds middleware to the chain.
func (mgr *Manager) Use(middleware ...MiddlewareFunc) {
	mgr.middleware = append(mgr.middleware, middleware...)
}

// NewManager returns a new manager with default values.
func NewManager() *Manager {
	return &Manager{
		Concurrency: 20,
		Logger:      NewStdLogger(),

		queues:         []string{"default"},
		done:           make(chan interface{}),
		shutdownWaiter: &sync.WaitGroup{},
		jobHandlers:    map[string]Handler{},
		eventHandlers: map[eventType][]func(){
			Startup:  []func(){},
			Quiet:    []func(){},
			Shutdown: []func(){},
		},
		weightedPriorityQueuesEnabled: false,
		weightedQueues:                []string{},
	}
}

// Run starts processing jobs.
// This method does not return.
func (mgr *Manager) Run() {
	// This will signal to Faktory that all connections from this process
	// are worker connections.
	rand.Seed(time.Now().UnixNano())
	faktory.RandomProcessWid = strconv.FormatInt(rand.Int63(), 32)

	if mgr.Pool == nil {
		pool, err := NewChannelPool(0, mgr.Concurrency, func() (Closeable, error) { return faktory.Open() })
		if err != nil {
			panic(err)
		}
		mgr.Pool = pool
	}

	mgr.fireEvent(Startup)

	go heartbeat(mgr)

	for i := 0; i < mgr.Concurrency; i++ {
		go process(mgr, i)
	}

	sigchan := hookSignals()

	for {
		sig := <-sigchan
		handleEvent(signalMap[sig], mgr)
	}
}

// One of the Process*Queues methods should be called once before Run()
func (mgr *Manager) ProcessStrictPriorityQueues(queues ...string) {
	mgr.queues = queues
	mgr.weightedPriorityQueuesEnabled = false
}

func (mgr *Manager) ProcessWeightedPriorityQueues(queues map[string]int) {
	uniqueQueues := queueKeys(queues)
	weightedQueues := expandWeightedQueues(queues)

	mgr.queues = uniqueQueues
	mgr.weightedQueues = weightedQueues
	mgr.weightedPriorityQueuesEnabled = true
}

func (mgr *Manager) queueList() []string {
	if mgr.weightedPriorityQueuesEnabled {
		sq := shuffleQueues(mgr.weightedQueues)
		return uniqQueues(len(mgr.queues), sq)
	}
	return mgr.queues
}

func heartbeat(mgr *Manager) {
	mgr.shutdownWaiter.Add(1)
	timer := time.NewTicker(5 * time.Second)
	for {
		select {
		case <-timer.C:
			// we don't care about errors, assume any network
			// errors will heal eventually
			_ = mgr.with(func(c *faktory.Client) error {
				data, err := c.Beat()
				if err != nil || data == "" {
					return err
				}
				var hash map[string]string
				err = json.Unmarshal([]byte(data), &hash)
				if err != nil {
					return err
				}

				if hash["state"] == "terminate" {
					handleEvent(Shutdown, mgr)
				} else if hash["state"] == "quiet" {
					handleEvent(Quiet, mgr)
				}
				return nil
			})
		case <-mgr.done:
			timer.Stop()
			mgr.shutdownWaiter.Done()
			return
		}
	}
}

func handleEvent(sig eventType, mgr *Manager) {
	switch sig {
	case Shutdown:
		go func() {
			mgr.Terminate()
		}()
	case Quiet:
		go func() {
			mgr.Quiet()
		}()
	}
}

func process(mgr *Manager, idx int) {
	mgr.shutdownWaiter.Add(1)
	// delay initial fetch randomly to prevent thundering herd.
	time.Sleep(time.Duration(rand.Int31()))
	defer mgr.shutdownWaiter.Done()

	for {
		if mgr.quiet {
			return
		}

		// check for shutdown
		select {
		case <-mgr.done:
			return
		default:
		}

		// fetch job
		var job *faktory.Job
		var err error

		err = mgr.with(func(c *faktory.Client) error {
			job, err = c.Fetch(mgr.queueList()...)
			if err != nil {
				return err
			}
			return nil
		})

		if err != nil {
			mgr.Logger.Error(err)
			time.Sleep(1 * time.Second)
			continue
		}

		// execute
		if job != nil {
			perform := mgr.jobHandlers[job.Type]
			if perform == nil {
				mgr.with(func(c *faktory.Client) error {
					return c.Fail(job.Jid, fmt.Errorf("No handler for %s", job.Type), nil)
				})
			} else {
				h := perform
				for i := len(mgr.middleware) - 1; i >= 0; i-- {
					h = mgr.middleware[i](h)
				}

				err := h(ctxFor(job), job)
				mgr.with(func(c *faktory.Client) error {
					if err != nil {
						return c.Fail(job.Jid, err, nil)
					} else {
						return c.Ack(job.Jid)
					}
				})
			}
		} else {
			// if there are no jobs, Faktory will block us on
			// the first queue, so no need to poll or sleep
		}
	}
}

func (mgr *Manager) fireEvent(event eventType) {
	for _, fn := range mgr.eventHandlers[event] {
		fn()
	}
}

// DefaultContext embeds Go's standard context and associates it with a job ID.
type DefaultContext struct {
	context.Context

	JID  string
	Type string
}

// Jid returns the job ID for the default context
func (c *DefaultContext) Jid() string {
	return c.JID
}

// JobType returns the job type for the default context
func (c *DefaultContext) JobType() string {
	return c.Type
}

func ctxFor(job *faktory.Job) Context {
	return &DefaultContext{
		Context: context.Background(),
		JID:     job.Jid,
		Type:    job.Type,
	}
}

func (mgr *Manager) with(fn func(fky *faktory.Client) error) error {
	conn, err := mgr.Pool.Get()
	if err != nil {
		return err
	}
	pc := conn.(*PoolConn)
	f, ok := pc.Closeable.(*faktory.Client)
	if !ok {
		return fmt.Errorf("Connection is not a Faktory client instance: %+v", conn)
	}
	err = fn(f)
	if err != nil {
		pc.MarkUnusable()
	}
	conn.Close()
	return err
}

// expandWeightedQueues builds a slice of queues represented the number of times equal to their weights.
func expandWeightedQueues(queueWeights map[string]int) []string {
	weightsTotal := 0
	for _, queueWeight := range queueWeights {
		weightsTotal += queueWeight
	}

	weightedQueues := make([]string, weightsTotal)
	fillIndex := 0

	for queue, nTimes := range queueWeights {
		// Fill weightedQueues with queue n times
		for idx := 0; idx < nTimes; idx++ {
			weightedQueues[fillIndex] = queue
			fillIndex++
		}
	}

	// weightedQueues has to be stable so we can write tests
	sort.Strings(weightedQueues)
	return weightedQueues
}

func queueKeys(queues map[string]int) []string {
	keys := make([]string, len(queues))
	i := 0
	for k := range queues {
		keys[i] = k
		i++
	}
	// queues has to be stable so we can write tests
	sort.Strings(keys)
	return keys
}

// shuffleQueues returns a copy of the slice with the elements shuffled.
func shuffleQueues(queues []string) []string {
	wq := make([]string, len(queues))
	copy(wq, queues)

	rand.Shuffle(len(wq), func(i, j int) {
		wq[i], wq[j] = wq[j], wq[i]
	})

	return wq
}

// uniqQueues returns a slice of length len, of the unique elements while maintaining order.
// The underlying array is modified to avoid allocating another one.
func uniqQueues(len int, queues []string) []string {
	// Record the unique values and position.
	pos := 0
	uniqMap := make(map[string]int)
	for _, v := range queues {
		if _, ok := uniqMap[v]; !ok {
			uniqMap[v] = pos
			pos++
		}
	}

	// Reuse the copied array, by updating the values.
	for queue, position := range uniqMap {
		queues[position] = queue
	}

	// Slice only what we need.
	return queues[:len]
}
