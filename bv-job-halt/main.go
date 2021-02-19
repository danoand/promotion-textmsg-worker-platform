// canceljob is a web service that indicates if a worker job should not run
// the service accepts a key (promotionsend document id, job name) and return boolean or no value
package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/danoand/utils"
	"github.com/gin-gonic/gin"
	"github.com/gomodule/redigo/redis"
	"github.com/kelseyhightower/envconfig"
)

// Specification defines an object to house environment variable values
type Specification struct {
	RedisURL string `required:"false"`
}

const cntDirectiveAll = "all" // 'all' jobs should stop

var (
	err error
	cfg Specification

	// Redis pool
	pool *redis.Pool
)

// newPool generates a new Redis pool
func newPool() *redis.Pool {
	return &redis.Pool{
		MaxIdle:     5,
		IdleTimeout: 270 * time.Second,
		Dial:        func() (redis.Conn, error) { return redis.DialURL(cfg.RedisURL) },
	}
}

// checkKey checks to see if the halt directive exists as a key in the Redis database (if yes... halt job)
func checkKey(kys []string) (bool, string) {
	var err error
	var val int

	conn := pool.Get()
	defer conn.Close()

	// Is there an 'all' directive? If so, all jobs in every environment should not proceed
	val, err = redis.Int(conn.Do("EXISTS", cntDirectiveAll))
	if err != nil {
		// error occurred checking Redis for the 'all' directive key
		log.Printf("ERROR: %v - error occurred checking Redis for the 'all' directive key. See: %v\n",
			utils.FileLine(),
			err)
		return false, "an error occurred" // error occurred - don't halt the job
	}

	// Does the 'all' directive exist?
	if val == 1 {
		return true, "all directive - halt all jobs"
	}

	// Iterate through the inbound parameters (inbound directives to be checked)
	for _, v := range kys {
		val, err = redis.Int(conn.Do("EXISTS", v))
		if err != nil {
			// error occurred checking Redis for a directive key
			log.Printf("ERROR: %v - error occurred checking Redis for the %v directive key. See: %v\n",
				utils.FileLine(),
				v,
				err)
			continue
		}

		// Does the current directive exist?
		if val == 1 {
			log.Printf("HALT: %v - found job directive: %v and returning to caller\n",
				utils.FileLine(),
				v)
			return true, fmt.Sprintf("halt job - %v directive has been found", v)
		}
	}

	return false, "no halt directive(s) found"
}

// getStatus gets status from the Redis database
func getStatus() (string, error) {
	var (
		err error
		val string
	)

	conn := pool.Get()
	defer conn.Close()

	// Is there an 'all' directive? If so, all jobs in every environment should not proceed
	val, err = redis.String(conn.Do("INFO", "stats"))
	if err != nil {
		// error occurred checking Redis for database stats
		log.Printf("ERROR: %v - error occurred checking Redis for database stats. See: %v\n",
			utils.FileLine(),
			err)
		return "error occurred checking Redis for database stats", err // error occurred - don't halt the job
	}

	return val, err
}

// hndlGetHaltDirective determines if the referring job should be halted given a set of inbound directives
//   return object...
//     "halt": <boolean> indicate if the referring job sould be halted
//     "msg":  <string>  message string
func hndlGetHaltDirective(c *gin.Context) {
	var (
		err    error
		bbytes []byte
		drts   []string
		rMap   = make(map[string]interface{})
	)

	rMap["halt"] = false
	rMap["msg"] = ""

	// Grab the inbound parameter - a json array with at least one element (the rest are ignored)
	bbytes, err = c.GetRawData()
	if err != nil {
		// error fetching the request body
		log.Printf("ERROR: %v - error fetching the request body. See: %v\n",
			utils.FileLine(),
			err)
		rMap["msg"] = "error processing the inbound request"
		c.JSON(http.StatusInternalServerError, rMap)
		return
	}

	// Parse the data as a json array
	err = utils.FromJSONBytes(bbytes, &drts)
	if err != nil {
		// error parsing the inbound json parameter
		log.Printf("ERROR: %v - error parsing the inbound json parameter. See: %v\n",
			utils.FileLine(),
			err)
		rMap["msg"] = "error parsing the inbound json request data"
		c.JSON(http.StatusInternalServerError, rMap)
		return
	}

	// Validate the data
	if len(drts) == 0 {
		// no directives found
		log.Printf("ERROR: %v - no inbound directives (array of strings) found - nothing to do\n",
			utils.FileLine())
		rMap["msg"] = "no inbound directives (array of strings) found - nothing to do"
		c.JSON(http.StatusBadRequest, rMap)
		return
	}

	// Check for directives indicating jobs that should be halted
	rMap["halt"], rMap["msg"] = checkKey(drts)

	msgType := "INFO"
	if rMap["halt"] == true {
		msgType = "HALT"
	}

	log.Printf("%v: %v - returning to the halt job caller with value: %v and message: %v\n",
		msgType,
		utils.FileLine(),
		rMap["halt"],
		rMap["msg"])

	c.JSON(http.StatusOK, rMap)
}

func main() {
	// Grab environment variables
	log.Printf("INFO: %v - fetching local environment variables\n", utils.FileLine())
	err = envconfig.Process("bvapp", &cfg)
	if err != nil {
		// error fetching and parsing environment variable data
		log.Printf("FATAL: %v - error fetching and parsing environment variable data. See: %v\n", utils.FileLine(), err)
		os.Exit(1)
	}

	// Started using Heroku Redis add-on: fetch the Heroku redis URL
	redisURL := os.Getenv("REDIS_URL")
	if len(redisURL) == 0 {
		// no REDIS_URL environment variable found
		log.Printf("FATAL: %v - no REDIS_URL environment variable found\n", utils.FileLine())
		os.Exit(1)
	}

	// Assign the Redis URL to the environment object
	cfg.RedisURL = redisURL

	// Grab the port environment variable for Heroku processing
	port := os.Getenv("PORT")
	if port == "" {
		log.Fatal("FATAL: $PORT must be set for Heroku processing\n")
	}

	pool = newPool()

	// Configure the web server and routes
	rtr := gin.Default()

	// rtr.GET("/", hndlServiceUI)                    // Serve the web page
	rtr.POST("/checkhalt", hndlGetHaltDirective) // Set a halt directive associated with an id
	rtr.GET("/status", func(c *gin.Context) {
		rslt, err := getStatus()
		if err != nil {
			c.String(http.StatusInternalServerError, rslt)
		}

		c.String(http.StatusOK, rslt)
	})

	// Start the web server
	log.Printf("INFO: %v - starting web server on port: %v\n",
		utils.FileLine(),
		port)

	rtr.Run(fmt.Sprintf(":%v", port))
}
