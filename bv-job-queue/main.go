// nqBroadcast is web service that receives requests to queue jobs to send
//   Promotion Broadcast text messages
package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/danoand/utils"
	"github.com/gin-gonic/gin"
	"github.com/kelseyhightower/envconfig"

	// mgo "gopkg.in/mgo.v2"
	mgo "github.com/globalsign/mgo"
	bson "github.com/globalsign/mgo/bson"

	fak "github.com/contribsys/faktory/client"

	cmn "github.com/whopdan/wrkrcom"
)

// Specification defines an object to house environment variable values
type Specification struct {
	WorkerEnvironment      string `required:"true"` // environment this worker is associated with (e.g. 'production')
	WorkerEnqueuePort      int    `default:"8080"`  // not referenced if running on Heroku
	WorkerDBURL            string `required:"true"`
	WorkerDBName           string `required:"true"`
	WorkerDBUser           string `required:"true"`
	WorkerDBPassword       string `required:"true"` // staging environment database configuration
	WorkerMsgThreshold     int    `default:"500"`
	WorkerJobTimeoutSec    int    `default:"7200"`
	WorkerJobDelaySec      int    `default:"1800"`
	WorkerPapertrailDomain string `default:"logs.papertrailapp.com:27834"`
	WorkerPapertrailApp    string `default:"bvenqueue"`
}

// ProcessDirective houses the job instructions read and parsed from a gridfile
type ProcessDirective struct {
	ID                     bson.ObjectId       `bson:"_id" json:"docid"`                                     // document object id
	IDString               string              `bson:"idstring" json:"idstring"`                             // document object id as a string
	PromoSendID            string              `json:"promosenddocid"`                                       // document id of the broadcast being processed
	JobID                  string              `json:"jobid"`                                                // job id
	Message                string              `json:"message"`                                              // the message text to be sent to end customers
	MediaURL               string              `json:"mediaurl"`                                             // media url referencing the text image
	Environment            string              `json:"environment"`                                          // request environment (e.g. prod)
	Data                   []map[string]string `json:"data"`                                                 // data being processed
	EnqueueTime            string              `json:"enqueuetime"`                                          // Time the directive was associated with a job and queued (not necessarily run)
	IsXPathwayGenericPromo bool                `bson:"isxpathwaygenericpromo" json:"isxpathwaygenericpromo"` // indicates if the underlying promotion is an xPathway generic promotion
}

// WorkerJobLogLine models a line to be logged capturing detail of a job run
type WorkerJobLogLine struct {
	ID             bson.ObjectId `bson:"_id" json:"docid"`
	PromoSendID    bson.ObjectId `bson:"promosenddocid" json:"promosenddocid"`
	JobID          string        `bson:"jobid" json:"jobid"`
	SequenceNumber string        `bson:"sequencenumber" json:"sequencenumber"`
	LogLine        string        `bson:"logline" json:"logline"`
}

const appName = "NQSVC"

var (
	err    error
	cfg    Specification
	port   string
	msgLim int

	mgoURL                  string
	mgoSession              *mgo.Session
	mgoDB                   *mgo.Database
	mgoGridFiles            *mgo.GridFS
	mgoCollJobQueueEnqueues *mgo.Collection
	mgoCollWorkerJobData    *mgo.Collection
	mgoCollWorkerJobLog     *mgo.Collection

	// Access common worker functions
	cmnWrkr cmn.Domain

	// Time variables
	timePT *time.Location
)

// appLog logs messages
func appLog(format string, v ...interface{}) {
	var cnt, msg string

	// Construct the logging line content
	cnt = fmt.Sprintf(format, v...)
	msg = fmt.Sprintf("App: %v (%v) - %v",
		appName,
		cfg.WorkerEnvironment,
		cnt)

	// Log locally
	log.Printf(msg)

	return
}

// jobLog is a function that writes log type lines to a slice
func jobLog(slc []string, format string, v ...interface{}) []string {
	// Format a string as a log 'line' and append to a slice that serves as a 'log'
	slc = append(slc, fmt.Sprintf("JOBLOG: %v", fmt.Sprintf(format, v...)))

	return slc
}

// wrtJobLog writes a slice of log 'lines' to a log collection in MongoDB
func wrtJobLog(env string, slc []string, pid string, jbid string) {

	// Iterate through the slice of "lines"
	for i := 0; i < len(slc); i++ {
		var tline WorkerJobLogLine
		var istr string

		// Construct the log "line"
		tline.ID = bson.NewObjectId()
		tline.JobID = jbid
		tline.LogLine = slc[i]
		tline.PromoSendID = bson.ObjectIdHex(pid)

		// Construct sequence string
		istr = fmt.Sprintf("%v", i)
		if len(istr) < 5 {
			istr = fmt.Sprintf("%v%v", "00000"[:5-len(istr)], i)
		}

		tline.SequenceNumber = fmt.Sprintf("%v_%v", time.Now().UTC().Format(time.RFC3339Nano), istr)

		err = mgoCollWorkerJobLog.Insert(tline)
		if err != nil {
			// error occurred inserting a log line
			appLog("ERROR: %v (%v) - error occurred inserting a log line. See: %v\n",
				utils.FileLine(),
				cfg.WorkerEnvironment,
				err)
		}
	}

	return
}

// postJob enqueues a job to fire out a Promo Send's SMS messages
func postJob(drt ProcessDirective, delayJb bool, delaySecs int) (string, error) {
	var (
		err error
		dur time.Duration
	)

	dur = time.Duration(delaySecs) * time.Second

	appLog("DEBUG: %v - attempting to submit job for psend: %v using delay?: %v\n",
		utils.FileLine(),
		drt.PromoSendID,
		delayJb)

	// Light validation of the inbound directive - do we have data?
	if len(drt.Data) == 0 || !bson.IsObjectIdHex(drt.IDString) {
		// missing job data or invalid document id
		return "", fmt.Errorf("missing job data or invalid document id - nothing to do")
	}

	job := fak.NewJob(cmn.CntWorkerNameSendSMSMessages, drt.PromoSendID, drt.IDString, drt.Environment) // Create a new job
	if delayJb {
		// Delay the execution of the job on the queue
		job.At = time.Now().UTC().Add(dur).Format(time.RFC3339Nano)
	}
	job.ReserveFor = cfg.WorkerJobTimeoutSec     // Assign a timeout
	job.Queue = cmn.CntWorkerNameSendSMSMessages // Post to the "bv-job-worker-smsmsgs" queue
	job.Retry = -1                               // Failed jobs should are not retried and moved to the 'Dead' Faktory tab

	// Assign the job id to the job directive
	drt.JobID = job.Jid

	// Store the job directive to the database
	err = mgoCollWorkerJobData.Insert(drt)
	if err != nil {
		// error inserting job directive data to the database
		appLog("ERROR: %v - error inserting job directive data to the database. See: %v\n",
			utils.FileLine(),
			err)
		return "", fmt.Errorf("error inserting job directive data to the database; see: %v", err)
	}

	err = cmnWrkr.FakClient.Push(job)
	if err != nil {
		// error pushing a faktory job
		appLog("ERROR: %v - error pushing a faktory job: %v (%v). See: %v\n",
			utils.FileLine(),
			cmn.CntWorkerNameSendSMSMessages,
			job.Jid,
			err)
		return "", fmt.Errorf("error pushing a faktory job: %v (%v); see: %v",
			job.Jid,
			cmn.CntWorkerNameSendSMSMessages,
			err)
	}

	appLog("INFO: %v - submitted job: %v (%v) for promotion send id: %v\n",
		utils.FileLine(),
		cmn.CntWorkerNameSendSMSMessages,
		job.Jid,
		drt.PromoSendID)

	return job.Jid, nil
}

// postAdHocQRGenJob enqueues a job to generate and save a QR code image
func postAdHocQRGenJob(shrtcde string, delayJb bool, delaySecs int) (string, error) {
	var (
		err error
		dur time.Duration
	)

	dur = time.Duration(delaySecs) * time.Second

	appLog("DEBUG: %v - attempting to submit job for shortcode: %v using delay?: %v\n",
		utils.FileLine(),
		shrtcde,
		delayJb)

	// Light validation of the inbound directive - do we have data?
	if len(shrtcde) == 0 {
		// missing job data
		return "", fmt.Errorf("missing job data - nothing to do")
	}

	job := fak.NewJob(cmn.CntWorkerNameQRCodeGen, "adhoc", shrtcde) // Create a new job
	if delayJb {
		// Delay the execution of the job on the queue
		job.At = time.Now().UTC().Add(dur).Format(time.RFC3339Nano)
	}
	job.ReserveFor = cfg.WorkerJobTimeoutSec // Assign a timeout
	job.Queue = cmn.CntWorkerNameQRCodeGen   // Post to the "bv-job-worker-qrcode-gen" queue
	job.Retry = -1                           // Failed jobs should are not retried and moved to the 'Dead' Faktory tab

	err = cmnWrkr.FakClient.Push(job)
	if err != nil {
		// error pushing a faktory job
		appLog("ERROR: %v - error pushing a faktory job: %v (%v). See: %v\n",
			utils.FileLine(),
			cmn.CntWorkerNameQRCodeGen,
			job.Jid,
			err)
		return "", fmt.Errorf("error pushing a faktory job: %v (%v); see: %v",
			job.Jid,
			cmn.CntWorkerNameQRCodeGen,
			err)
	}

	appLog("INFO: %v - submitted job: %v (%v) for shortcode: %v\n",
		utils.FileLine(),
		cmn.CntWorkerNameQRCodeGen,
		job.Jid,
		shrtcde)

	return job.Jid, nil
}

// hdlQueueSMSJob handles inbound requests to enqueue SMS message jobs to a Faktory worker
func hdlQueueSMSJob(c *gin.Context) {
	var (
		err             error
		fname           string
		rbytes          []byte
		tMap            = make(map[string]string)
		logLines        []string
		drtive, tmpDir  ProcessDirective
		ctr, runngDelay int
		jobCtr, numCtr  = 1, 1
		delayJob        bool
	)

	// Read the request body
	rbytes, err = c.GetRawData()
	if err != nil {
		// error reading the request data
		appLog("ERROR: %v - error reading the request data. See: %v\n",
			utils.FileLine(),
			err)
		c.JSON(http.StatusBadRequest, fmt.Sprintf("error reading the request data; see: %v", err))
		return
	}

	// Parse the inbound request data
	err = utils.FromJSONBytes(rbytes, &tMap)
	if err != nil {
		// error parsing the request data
		appLog("ERROR: %v - error parsing the request data. See: %v\n",
			utils.FileLine(),
			err)
		c.JSON(http.StatusBadRequest, fmt.Sprintf("error parsing the request data; see: %v", err))
		return
	}

	appLog("INFO: %v - handling a new inbound request with request body:\n%v\n", utils.FileLine(), string(rbytes))

	// Validate the request data
	if len(tMap["docid"]) == 0 {
		// missing parameter data
		appLog("ERROR: %v - missing docid parameter data\n", utils.FileLine())
		c.JSON(http.StatusBadRequest, fmt.Sprintf("missing docid parameter"))
		return
	}

	if !bson.IsObjectIdHex(tMap["docid"]) {
		// invalid document id
		appLog("ERROR: %v - invalid document id: %v\n",
			utils.FileLine(),
			tMap["docid"])
		c.JSON(http.StatusBadRequest, fmt.Sprintf("invalid document id"))
		return
	}

	appLog("INFO: %v - inbound request for psend: %v in environment: %v\n",
		utils.FileLine(),
		tMap["docid"],
		cfg.WorkerEnvironment)

	// Fetch the enqueue data from the database
	err = mgoCollJobQueueEnqueues.Find(bson.M{"idstring": tMap["docid"]}).One(&drtive)
	if err != nil {
		// error fetching job enqueuing data
		appLog("ERROR: %v - error fetching job enqueuing data: %v. See: %v\n",
			utils.FileLine(),
			tMap["docid"],
			err)
		c.JSON(http.StatusBadRequest, fmt.Sprintf("error fetching job enqueuing data; see: %v", err))
		return
	}

	appLog("DEBUG: %v - enqueuing jobs with this information: psend: %v message: %v\n",
		utils.FileLine(),
		drtive.PromoSendID,
		drtive.Message)

	//* Validate the unmarshaled data

	// Missing data?
	if len(drtive.PromoSendID) == 0 ||
		len(drtive.Data) == 0 ||
		len(drtive.Message) == 0 {
		// missing broadcast document id, end customer data, or message text
		appLog("ERROR: %v - missing broadcast document id, end customer data, or message text\n", utils.FileLine())
		c.JSON(http.StatusBadRequest, "missing broadcast document id, end customer data, or message text")
		return
	}

	// Invalid document ids?
	if !bson.IsObjectIdHex(drtive.PromoSendID) {
		// invalid document id
		appLog("ERROR: %v - invalid document id - %v\n", utils.FileLine(), drtive.PromoSendID)
		c.JSON(http.StatusBadRequest, fmt.Sprintf("invalid document id - %v", drtive.PromoSendID))
		return
	}

	// Iterate through the data and break up into chunks if necessary
	tmpDir = ProcessDirective{}
	tmpDir.PromoSendID = drtive.PromoSendID
	tmpDir.Message = drtive.Message
	tmpDir.MediaURL = drtive.MediaURL
	tmpDir.IsXPathwayGenericPromo = drtive.IsXPathwayGenericPromo
	ctr = 1        // phone number counter - determine if we're at our threshold of Twilio calls
	runngDelay = 0 // running job delay for multiple jobs in seconds
	for k, v := range drtive.Data {
		var jbID string

		// Exceed the processing threshold?
		if ctr > cfg.WorkerMsgThreshold {
			// Post (enqueue) a job to fire out a Promo Send's SMS messages
			if jobCtr != 1 {
				delayJob = true
				runngDelay = runngDelay + cfg.WorkerJobDelaySec
			}
			tmpDir.ID = bson.NewObjectId()    // Assign a unique document id for this object (to be stored in the database)
			tmpDir.IDString = tmpDir.ID.Hex() // string instance of the document id
			tmpDir.EnqueueTime = time.Now().In(timePT).Format(time.RFC3339Nano)
			jbID, err = postJob(tmpDir, delayJob, runngDelay)

			// log activity
			if err != nil {
				// error occurred submitting a job
				appLog("ERROR: %v - error submitting job #%v\n", utils.FileLine(), jobCtr)
				jobLog(logLines, "ERROR: error submitting job #%v", jobCtr)
			}
			if err == nil {
				// successfully submitted a job
				appLog("INFO: %v - submitted job #%v\n", utils.FileLine(), jobCtr)
				jobLog(logLines, "INFO: submitted job #%v with job id: %v", jobCtr, jbID)
			}

			// Reset the data for next chunk
			tmpDir.Data = []map[string]string{}
			jobCtr = jobCtr + 1
			ctr = 1 // phone number counter - determine if we're at our threshold of Twilio requests
		}

		// Validate the data
		if len(v["documentid"]) == 0 || len(v["phonenumber"]) == 0 {
			// missing end customer document id or phone number
			appLog("ERROR: %v - data line %v is missing the end customer document id or phone number\n", utils.FileLine(), k)
			jobLog(logLines, "ERROR: data line %v is missing the end customer document id or phone number", k)

			continue
		}

		// Add data to the object for the next job invocation
		tmpDir.Data = append(tmpDir.Data, v)
		numCtr = numCtr + 1 // total number of phones texts (total # of texts)
		ctr = ctr + 1       // phone number counter - determine if we're at our threshold of Twilio requests
	}

	// Post (enqueue) a job to fire out the Promo Send's remaining SMS messages
	var jbID string
	if jobCtr != 1 {
		delayJob = true
		runngDelay = runngDelay + cfg.WorkerJobDelaySec
	}
	tmpDir.ID = bson.NewObjectId()    // Assign a unique document id for this object (to be stored in the database)
	tmpDir.IDString = tmpDir.ID.Hex() // string instance of the document id
	tmpDir.EnqueueTime = time.Now().In(timePT).Format(time.RFC3339Nano)
	jbID, err = postJob(tmpDir, delayJob, runngDelay)
	// log activity
	if err != nil {
		// error occurred submitting a job
		appLog("ERROR: %v - error submitting job #%v\n", utils.FileLine(), jobCtr)
		jobLog(logLines, "ERROR: error submitting job #%v", jobCtr)
	}
	if err == nil {
		// successfully submitted a job
		appLog("INFO: %v - submitted job #%v\n", utils.FileLine(), jobCtr)
		jobLog(logLines, "INFO: submitted job #%v with job id: %v", jobCtr, jbID)
	}

	// Write the "log lines" to the database
	wrtJobLog(tmpDir.Environment, logLines, drtive.PromoSendID, jbID) // Write to the Promotion Send's log

	// return to the caller
	appLog("INFO: %v - returning to caller with psend: %v after enqueuing %v jobs with %v messages and wrote the job log to: %v\n",
		utils.FileLine(),
		drtive.PromoSendID,
		jobCtr,
		numCtr,
		fname)
	c.JSON(http.StatusOK, fmt.Sprintf("successfully enqueued %v jobs sending %v messages", jobCtr, numCtr))
}

// hdlQueueAdHocQRGenJob handles an inbound request to enqueue a job to generate and store a QR code for a shortcode
//   intended for forwarded/shared promotions
func hdlQueueAdHocQRGenJob(c *gin.Context) {
	var (
		err    error
		jbID   string
		rbytes []byte
		tMap   = make(map[string]string)
	)

	// Read the request body
	rbytes, err = c.GetRawData()
	if err != nil {
		// error reading the request data
		appLog("ERROR: %v - error reading the request data. See: %v\n",
			utils.FileLine(),
			err)
		c.JSON(http.StatusBadRequest, fmt.Sprintf("error reading the request data; see: %v", err))
		return
	}

	// Parse the inbound request data
	err = utils.FromJSONBytes(rbytes, &tMap)
	if err != nil {
		// error parsing the request data
		appLog("ERROR: %v - error parsing the request data. See: %v\n",
			utils.FileLine(),
			err)
		c.JSON(http.StatusBadRequest, fmt.Sprintf("error parsing the request data; see: %v", err))
		return
	}

	appLog("INFO: %v - handling a new inbound request with request body:\n%v\n", utils.FileLine(), string(rbytes))

	// Validate the request data
	if len(tMap["shortcode"]) == 0 {
		// missing parameter data
		appLog("ERROR: %v - missing shortcode parameter data\n", utils.FileLine())
		c.JSON(http.StatusBadRequest, fmt.Sprintf("missing shortcode parameter"))
		return
	}

	appLog("INFO: %v - inbound ad hoc qr generation request for shortcode: %v in environment: %v\n",
		utils.FileLine(),
		tMap["shortcode"],
		cfg.WorkerEnvironment)

	jbID, err = postAdHocQRGenJob(tMap["shortcode"], false, 0)

	// log activity
	if err != nil {
		// error occurred submitting a job
		appLog("ERROR: %v - error submitting job #%v\n", utils.FileLine(), jbID)
		c.JSON(http.StatusInternalServerError, fmt.Sprintf("error submitting job #%v", jbID))
	}

	// return to the caller
	appLog("INFO: %v - returning to caller requesting to encode a QR code with shortcode: %v via job #%v\n",
		utils.FileLine(),
		tMap["shortcode"],
		jbID)
	c.JSON(http.StatusOK, fmt.Sprintf("successfully enqueued job: %v", jbID))
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

	// Grab the port environment variable for Heroku processing
	port := os.Getenv("PORT")
	if port == "" {
		log.Fatal("FATAL: $PORT must be set for Heroku processing\n")
	}

	// Load Pacific Time
	timePT, err = time.LoadLocation("America/Los_Angeles")
	if err != nil {
		// error loading the PT timezone
		log.Printf("FATAL: %v - error loading the PT timezone. See: %v\n",
			utils.FileLine(),
			err)
		os.Exit(1)
	}

	// Dial the production MongoDB database
	mgoURL = fmt.Sprintf(cfg.WorkerDBURL, cfg.WorkerDBUser, cfg.WorkerDBPassword)
	appLog("INFO: %v - connecting to the MongoDB database\n", utils.FileLine())
	mgoSession, err = mgo.Dial(mgoURL)
	if err != nil {
		// error dialing the MongoDB database
		appLog("FATAL: %v - fatal error dialing the hosted Mongodb database. See: %v\n", utils.FileLine(), err)
		os.Exit(1)
	}
	appLog("INFO: %v - successfully connected to the production MongoDB database\n", utils.FileLine())

	// Define the gridfs namespace to house files that support job processing
	mgoDB = mgoSession.DB(cfg.WorkerDBName)
	mgoCollJobQueueEnqueues = mgoDB.C("jobqueueenqueues") // collection holding the data used to submit individual worker jobs
	mgoCollWorkerJobData = mgoDB.C("workerjobdata")       // collection holding the data to be processed by individual worker jobs
	mgoCollWorkerJobLog = mgoDB.C("workerjoblog")         // collection holding log data/lines for a worker job run

	// Close the MongoDB session at the end of processing
	defer mgoSession.Close()

	appLog("INFO: %v - the dialed production database information is:\n%v\n\n",
		utils.FileLine(),
		mgoDB.Name)

	// Configure the common handler objects
	cmnWrkr, err = cmn.NewDomain()
	if err != nil {
		// error creating the common worker object
		//  ... used to inject db and Faktory objects
		appLog("ERROR: %v - error creating a Faktory client. See: %v\n",
			utils.FileLine(),
			err)
		os.Exit(1)
	}
	cmnWrkr.Mgo = mgoSession
	cmnWrkr.MgoDB = mgoDB

	// Ping non-production Heroku instances
	if cfg.WorkerEnvironment != "production" {
		cmn.WakeWorkerApps()
	}

	// Set up the web service routes
	r := gin.Default()
	r.POST("/enqueuejob", hdlQueueSMSJob)
	r.POST("/queueqrgen", hdlQueueAdHocQRGenJob) // route that handles worker jobs generating QR code shortcodes on an ad hoc basis (shared promos)
	r.GET("/status", cmnWrkr.HndlrStatus)

	// Fire goroutine used to execute cron jobs
	go schedJobs()

	appLog("INFO: %v - starting the enqueue web service: %v\n", utils.FileLine(), appName)
	// Start the web server
	err = r.Run(fmt.Sprintf(":%v", port))
	if err != nil {
		// error occurred starting the web server
		appLog("FATAL: %v - error occurred starting the web server. See: %v\n", utils.FileLine(), err)
	}
}
