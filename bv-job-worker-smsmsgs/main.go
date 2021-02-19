package main

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/gin-gonic/gin"
	mgo "github.com/globalsign/mgo"
	bson "github.com/globalsign/mgo/bson"

	"github.com/allegro/bigcache"
	faktory "github.com/contribsys/faktory/client"
	worker "github.com/contribsys/faktory_worker_go"
	"github.com/danoand/utils"
	"github.com/kelseyhightower/envconfig"

	cmn "github.com/whopdan/wrkrcom"
)

const (
	appName = "NQJOB"

	cntThreePipesPlaceholder     string = "|||"                           // placeholder that is ultimately replaced by a unique shortlink
	cntTwilioSkipStopPhoneNumber string = "twilio_skip_stop_phone_number" // send event to skip a text to a (stop) phone number
)

var (
	err error
	cfg Specification

	mgoURL                    string
	mgoSession                *mgo.Session
	mgoDB                     *mgo.Database
	mgoMgoCollSMSMessages     *mgo.Collection
	mgoCollWorkerJobLog       *mgo.Collection
	mgoCollWorkerJobData      *mgo.Collection
	mgoMgoCollPromoBroadcasts *mgo.Collection
	mgoCollShortLinkStore     *mgo.Collection
	mgoMgoCollPromoCustTexts  *mgo.Collection
	mgoCollStopPhoneList      *mgo.Collection

	// Twilio variables
	twilioURLElement1         = "https://"
	twilioURLElement2         = ":"
	twilioURLElement3         = "@api.twilio.com/2010-04-01/Accounts/"
	twilioURLElement4         = "/Messages.json"
	twilioConstructMessageURL string

	// Time location
	timePT     *time.Location
	timeLocStr = "America/Los_Angeles"

	// Access common worker functions
	cmnWrkr cmn.Domain
)

// isBVTest is a function that determines if the sms message is considered a Broadvibe test message
//   true:  will be processed even if outside the stated call hours
//   false: will be subject to the stated call hours
func isBVTest(msg string, env string) bool {
	return strings.Contains(msg, cfg.WorkerInternalBVTestFlag) || env == cntAppEnvDev
}

// jobLog is a function that writes log type lines to a slice
func jobLog(slc []string, format string, v ...interface{}) []string {
	// Format a string as a log 'line' and append to a slice that serves as a 'log'
	slc = append(slc, fmt.Sprintf("JOBLOG: %v", fmt.Sprintf(format, v...)))

	return slc
}

// wrtJobLog writes a slice of log 'lines' to a log collection in MongoDB
func wrtJobLog(slc []string, pid string, jbid string, env string) {

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

		// Insert the object into the MongoDB
		err = mgoCollWorkerJobLog.Insert(tline)
		if err != nil {
			// error occurred inserting a log line
			appLog("ERROR: %v - error occurred inserting a log line into the %v database. See: %v\n",
				utils.FileLine(),
				cfg.WorkerCurEnv,
				err)
		}
	}

	return
}

// appLog logs messages
func appLog(format string, v ...interface{}) {
	var cnt, msg string

	// Construct the logging line content
	cnt = fmt.Sprintf(format, v...)
	msg = fmt.Sprintf("App: %v - %v", appName, cnt)

	// Log locally
	log.Printf(msg)

	return
}

// logJobName appends a job name and error code to a promotionsend
func logJobName(id string, jb string, env string, er *error) {
	var (
		err   error
		docid bson.ObjectId
		str   string
	)

	// Validate paramaters
	if !bson.IsObjectIdHex(id) || len(jb) == 0 {
		// invalid document id or job id
		appLog("ERROR: %v - invalid document id: %v or job id: %v - can't log a job to a promotion broadcast\n",
			utils.FileLine(),
			id,
			jb)

		return
	}

	// Construct a document payload
	str = fmt.Sprintf("%v | %v", jb, *er)

	docid = bson.ObjectIdHex(id)

	// Log to the appropriate database
	err = mgoMgoCollPromoBroadcasts.UpdateId(docid, bson.M{"$push": bson.M{"psendworkerjobs": str}})
	if err != nil {
		// error appending a jobid to a promotion send
		appLog("ERROR: %v - error appending a jobid to a promotion send id: %v or job id: %v to the production database - can't log a job to a promotion broadcast\n",
			utils.FileLine(),
			id,
			jb)
	}

	return
}

// Specification defines an object to house environment variable values
type Specification struct {
	WorkerCurEnv                string `required:"true"`
	WorkerHaltJobCheckURL       string `default:"https://bv-job-halt.herokuapp.com/checkhalt"`
	WorkerLogSkipStopPhnNumURL  string `default:"https://xpathway-app-prod.herokuapp.com/clientevent"`
	WorkerDBURL                 string `required:"true"`
	WorkerDBName                string `required:"true"`
	WorkerDBUser                string `required:"true"`
	WorkerDBPassword            string `required:"true"`
	WorkerMsgThreshold          int    `default:"2000"`
	WorkerTwilioAccountSID      string `required:"true"`
	WorkerTwilioAuthToken       string `required:"true"`
	WorkerTwilioNumber          string `required:"true"`
	WorkerSMSDelaySec           int    `default:"1"`
	WorkerSMSHaltJobInterval    int    `default:"20"`    // Text message interval to check if job should be halted (e.g. check ever 10 msgs, etc.)
	WorkerProgressInterval      int    `default:"100"`   // Text message interval used to report progress
	WorkerStubTwilio            bool   `default:"false"` // false: MEANS SENDING LIVE TWILIO MESSAGES!
	WorkerStubTwilioTestURL     string `default:"https://echo.danocloud.com/stubtwilio"`
	WorkerMsgCheckIfInCallHours bool   `default:"false"`        // false: don't check if within call hours (i.e. send text at any time of day); true: check if within call hours
	WorkerMsgStartHour          string `default:"08"`           // hour of 24 hour day at which texts can be sent   (e.g. 8am or '08')
	WorkerMsgEndHour            string `default:"20"`           // hour of 24 hour day at which texts can't be sent (e.g. 10pm or '22')
	WorkerInternalBVTestFlag    string `default:"96JCAKZ7(7DN"` // string in the message text that indicates the text is an internal BV test
	WorkerJobTimeoutSec         int    `default:"7200"`         // job timeout in seconds
	ShortLinkBaseURL            string `default:"http://bdvi.be/x"`
	ShortLinkBaseURLDev         string `default:"http://localhost:8080/x"`
	ShortLinkBaseURLStg         string `default:"http://staging.bdvi.be/x"`
	WorkerRedisURL              string `required:"false"`
}

// ProcessDirective houses the job instructions
type ProcessDirective struct {
	PromoSendID            string              `bson:"promosendid" json:"promosendid"`                       // document id of the broadcast being processed
	Message                string              `bson:"message" json:"message"`                               // the message text to be sent to end customers
	MediaURL               string              `bson:"mediaurl" json:"mediaurl"`                             // media url referencing the text image
	Environment            string              `bson:"environment" json:"environment"`                       // request environment (e.g. prod)
	Data                   []map[string]string `bson:"data" json:"data"`                                     // data being processed
	IsXPathwayGenericPromo bool                `bson:"isxpathwaygenericpromo" json:"isxpathwaygenericpromo"` // indicates if the underlying promotion is an xPathway generic promotion
	TwilioOverridePhoneNum string              `bson:"twiliooverridephonenum" json:"twiliooverridephonenum"` // override the default Twilio Number with this number (if valid)
}

// WorkerJobLogLine models a line to be logged capturing detail of a job run
type WorkerJobLogLine struct {
	ID             bson.ObjectId `bson:"_id" json:"docid"`
	PromoSendID    bson.ObjectId `bson:"promosenddocid" json:"promosenddocid"`
	JobID          string        `bson:"jobid" json:"jobid"`
	SequenceNumber string        `bson:"sequencenumber" json:"sequencenumber"`
	LogLine        string        `bson:"logline" json:"logline"`
}

// WorkerHaltResponse models a response from the service which indicates if a job should be halted
type WorkerHaltResponse struct {
	Halt bool   `json:"halt"`
	Msg  string `json:"msg"`
}

// stopWorker is a function that checks a shared Heroku Redis instance to determine
//   if the worker job should not start or be halted (true = halt job)
func stopWorker(keys []string) bool {
	var (
		err    error
		dat    interface{}
		bbytes []byte
		brdr   *bytes.Reader
		rsp    *http.Response
		rslt   WorkerHaltResponse
	)

	dat = append([]string{cntAppEnvProd}, keys...)

	brdr, err = utils.ToJSONReader(&dat)
	if err != nil {
		// error occurred translating a json object to a reader
		log.Printf("ERROR: %v - error occurred translating a json object to a reader. See: %v\n",
			utils.FileLine(),
			err)
		return false
	}

	// Call out to the halt job check service
	rsp, err = http.Post(cfg.WorkerHaltJobCheckURL, "application/json", brdr)
	if err != nil {
		// error occurred making call to the halt job check service
		log.Printf("ERROR: %v - error occurred making call to the halt job check service. See: %v\n",
			utils.FileLine(),
			err)
		return false
	}

	// Read the response body from the halt service
	bbytes, err = ioutil.ReadAll(rsp.Body)
	if err != nil {
		// error occurred reading the response body from the halt service
		log.Printf("ERROR: %v - error occurred reading the response body from the halt service. See: %v\n",
			utils.FileLine(),
			err)
		return false
	}

	// Translate the response to a go object
	err = utils.FromJSONBytes(bbytes, &rslt)
	if err != nil {
		// error occurred translating the halt service response to a go object
		log.Printf("ERROR: %v - error occurred translating the halt service response to a go object. See: %v\n",
			utils.FileLine(),
			err)
		return false
	}

	if rslt.Halt {
		log.Printf("HALT: %v - current running job should be halted. See: %v\n",
			utils.FileLine(),
			rslt.Msg)
	}

	return rslt.Halt
}

// setPSendHalted flags a promotion broadcast as halted
func setPSendHalted(id, msg string) error {

	if !bson.IsObjectIdHex(id) {
		// id is not a valid document id
		return fmt.Errorf("%v is not a valid document id", id)
	}

	// Set the isHalted flag and a reason code (message)
	err = mgoMgoCollPromoBroadcasts.UpdateId(
		bson.ObjectIdHex(id),
		bson.M{"$set": bson.M{"ishalted": true, "psendhaltedmsg": msg}},
	)
	if err != nil {
		// error flagging the promotion as halted
		return fmt.Errorf("error flagging the promotion as halted: %v", err)
	}

	return nil
}

// inCallHours is a function that determines if the current time is in a time window (for sending texts) in the Pacific Timezone
func inCallHours() bool {
	// Checking to see if within call hours?
	//    - no  check (send texts at any time of day)
	//    - yes check (proceed and check time against call/text time window)
	if !cfg.WorkerMsgCheckIfInCallHours {
		// Call hour check disabled - send the message regardless of time (return true)
		return true
	}

	// Get the current time in the Pacific timezone
	crt := time.Now().In(timePT).Format(time.RFC3339Nano)
	crtHour := crt[11:13]

	// Is the current hour before the window start hour or after the window end hour?
	//   before the start hour - don't send text
	//   after the end hour    - don't send text
	if crtHour < cfg.WorkerMsgStartHour || crtHour >= cfg.WorkerMsgEndHour {
		// current hour is outside the call time window
		appLog("INFO: %v - current hour is outside the call time window\n", utils.FileLine())
		return false
	}

	// Inside the call window
	return true
}

// logSkipStopPhoneNumber is a function that sends an event to the web app to log skipping a stop phone number (ie. text not sent out to that number)
func logSkipStopPhoneNumber(twlPhnNum string) {
	var (
		err      error
		retMap   = make(map[string]interface{})
		datMap   = make(map[string]string)
		evtMap   = make(map[string]string)
		bytesMap []byte
	)

	// Construct data map elements
	datMap["docid"] = ""
	datMap["usertype"] = ""
	datMap["username"] = ""

	// Construct event map elements
	evtMap["event"] = cntTwilioSkipStopPhoneNumber
	evtMap["phone"] = twlPhnNum
	evtMap["timestamp"] = time.Now().In(timePT).Format(time.RFC3339)

	// Construct request body
	retMap["data"] = datMap
	retMap["event"] = evtMap

	// Transform the map to a json byte array
	_, bytesMap, err = utils.ToJSON(retMap)
	if err != nil {
		// error encoding the return map as json bytes
		appLog("ERROR: %v - error encoding the return map as json bytes. See: %v\n",
			utils.FileLine(),
			err)

		return
	}

	// Construct a bytes/io reader
	buf := bytes.NewBuffer(bytesMap)

	// Request an event logging from the main web application
	_, err = http.Post(cfg.WorkerLogSkipStopPhnNumURL, "application/json", buf)
	if err != nil {
		// error transmitting a request to log an event to skip over a stop phone number
		appLog("ERROR: %v - error transmitting a request to log an event (%v) to skip over a stop phone number. See: %v\n",
			utils.FileLine(),
			cfg.WorkerLogSkipStopPhnNumURL,
			err)
	}

	return
}

// wkrFirePromoSendSMSMsgs is a Faktory "job" that sends out a set of SMS messages associated
//   ... with a promotion broadcast
func wkrFirePromoSendSMSMsgs(fnCtx context.Context, args ...interface{}) error {
	var (
		err              error
		jberr            error // error returned when job ends
		flgGenPromo      bool
		loglines         []string
		psdid, jdtid     string
		phn, msg, reqURL string
		limit, ntrvlCtr  int
		drtv             ProcessDirective
		resp             *http.Response
		bBytes           []byte
		dur              = time.Duration(cfg.WorkerSMSDelaySec) * time.Second
		shrtlnk          *shortLinkObj
		job              *faktory.Job
		cache            *bigcache.BigCache
		mapCacheElms     []map[string]interface{}
		byteOne          = []byte("1")
	)

	// TODO: Note - looks like Faktory worker migrated to go Context contexts
	//    using this statement below to grab the details which were associated
	//    with the legacy Faktory worker context
	ctx := worker.HelperFor(fnCtx)

	loglines = jobLog(loglines, "Start working on job: %v", ctx.Jid())

	// Do we have three arguments?
	if len(args) < 3 {
		// missing job arguments
		appLog("ERROR: %v - JobId: %v - %v\n",
			utils.FileLine(),
			ctx.Jid(),
			"missing job arguments")

		loglines = jobLog(loglines, "missing job arguments")
		loglines = jobLog(loglines, "Job: %v ending due to an error", ctx.Jid())
		wrtJobLog(loglines, psdid, ctx.Jid(), cfg.WorkerCurEnv) // Write to the Promotion Send's log
		err = fmt.Errorf("missing job arguments")
		return err
	}

	psdid = fmt.Sprintf("%v", args[0])
	jdtid = fmt.Sprintf("%v", args[1])

	// Append the job name and error disposition to the database
	defer logJobName(psdid, ctx.Jid(), cfg.WorkerCurEnv, &jberr)

	// Check if job should be halted
	if stopWorker([]string{cfg.WorkerCurEnv, psdid, ctx.Jid()}) {
		appLog("HALT: %v - halting job: %v before it starts\n",
			utils.FileLine(),
			ctx.Jid())
		loglines = jobLog(loglines, "halting job: %v before processing began",
			ctx.Jid())

		wrtJobLog(loglines, psdid, ctx.Jid(), cfg.WorkerCurEnv) // Write to the Promotion Send's log
		err = setPSendHalted(psdid, fmt.Sprintf("halting job: %v before processing began", ctx.Jid()))
		if err != nil {
			// error flagging a promotion send as halted
			log.Printf("ERROR: %v - error flagging a promotion send %v as halted. See: %v\n",
				utils.FileLine(),
				psdid,
				err)
		}
		jberr = fmt.Errorf("job halted before processing began")

		return jberr
	}

	// Validate the inbound job arguments
	if !bson.IsObjectIdHex(psdid) || !bson.IsObjectIdHex(jdtid) {
		// one or more invalid document ids
		appLog("ERROR: %v - JobId: %v - %v psdid: %v jdtid: %v See: %v\n",
			utils.FileLine(),
			ctx.Jid(),
			"one or more invalid document ids",
			psdid,
			jdtid,
			err)

		loglines = jobLog(loglines, "one or more invalid document ids")
		loglines = jobLog(loglines, "Job: %v ending due to an error", ctx.Jid())
		wrtJobLog(loglines, psdid, ctx.Jid(), cfg.WorkerCurEnv) // Write to the Promotion Send's log
		jberr = fmt.Errorf("one or more invalid document id")
		return jberr
	}

	// Query the database for the job directive
	err = mgoCollWorkerJobData.Find(bson.M{"idstring": jdtid}).One(&drtv)
	if err != nil {
		// error fetching job directive data
		appLog("ERROR: %v - JobId: %v - %v. See: %v\n",
			utils.FileLine(),
			ctx.Jid(),
			"error fetching job directive data from the database",
			err)

		loglines = jobLog(loglines, "error fetching job directive data the database")
		loglines = jobLog(loglines, "Job: %v ending due to an error", ctx.Jid())
		wrtJobLog(loglines, psdid, ctx.Jid(), cfg.WorkerCurEnv) // Write to the Promotion Send's log
		jberr = fmt.Errorf("error fetching job directive data the database")
		return jberr
	}

	appLog("DEBUG: %v - the job drtv for psend: %v with twiliooverridephonenum: %v\n",
		utils.FileLine(),
		drtv.PromoSendID,
		drtv.TwilioOverridePhoneNum)

	// Validate the job data - missing execution data?
	if len(drtv.Data) == 0 || len(drtv.Message) == 0 || len(drtv.PromoSendID) == 0 {
		// missing job data
		appLog("ERROR: %v - JobId: %v - %v\n",
			utils.FileLine(),
			ctx.Jid(),
			"missing job data, text message body, or promotionsend document id")

		loglines = jobLog(loglines, "missing job data, text message body, or promotionsend document id")
		loglines = jobLog(loglines, "Job: %v ending due to an error", ctx.Jid())
		wrtJobLog(loglines, psdid, ctx.Jid(), cfg.WorkerCurEnv) // Write to the Promotion Send's log
		jberr = fmt.Errorf("missing job data or text message body")
		return jberr
	}
	limit = len(drtv.Data)
	if len(drtv.Data) > cfg.WorkerMsgThreshold {
		// data contains more the the allowed number of customers; capping at the threshold limit
		limit = cfg.WorkerMsgThreshold
		appLog("WARN: %v - JobId: %v - %v: %v\n",
			utils.FileLine(),
			ctx.Jid(),
			"data contains more the the allowed number of customers; capping at the threshold limit",
			limit)

		// Log warning message but keep processing
		loglines = jobLog(loglines, "data contains more the the allowed number of customers; capping at the threshold limit")
	}

	// Is this text message "firing" associated with an xPathway generic promotion?
	flgGenPromo = drtv.IsXPathwayGenericPromo
	if flgGenPromo {
		loglines = jobLog(loglines, "processing an xPathway generic promotion")
	}

	// SHOULD JOB BE HALTED?
	//   1. ARE WE OUT OF THE CALL HOURS? and
	//   2. THIS IS NOT AN INTERNAL BROADVIBE TEST (internal Broadvibe tests are excepted from the call hour restriction... use wisely!)
	if !inCallHours() && !isBVTest(drtv.Message, cfg.WorkerCurEnv) {
		// not in call window - halt (don't run) the job
		appLog("INFO: %v - halting job: %v current time is outside of the call hours\n", utils.FileLine(), ctx.Jid())
		loglines = jobLog(loglines, "halting job: %v current time is outside of the call hours", ctx.Jid())
		jberr = fmt.Errorf("job is running outside of the call hours")

		return jberr
	}

	// Construct a cache to house stop phone numbers
	appLog("INFO: %v - JobId: %v - %v\n",
		utils.FileLine(),
		ctx.Jid(),
		"creating a cache to store stop phone numbers for use during this job")

	cache, err = bigcache.NewBigCache(bigcache.DefaultConfig(60 * time.Minute))
	if err != nil {
		// error creating a cache object
		appLog("ERROR: %v - JobId: %v - %v. See: %v\n",
			utils.FileLine(),
			ctx.Jid(),
			"error creating a cache object",
			err)

		loglines = jobLog(loglines, "error creating a cache object")
		loglines = jobLog(loglines, "Job: %v ending due to an error", ctx.Jid())
		wrtJobLog(loglines, psdid, ctx.Jid(), cfg.WorkerCurEnv) // Write to the Promotion Send's log
		jberr = fmt.Errorf("error creating a cache object")
		return jberr
	}

	// Query the database for the stop phone numbers
	err = mgoCollStopPhoneList.Find(bson.M{"stopmessages": true}).All(&mapCacheElms)
	if err != nil && err != mgo.ErrCursor {
		// error fetching stop phone numbers from the database
		appLog("ERROR: %v - JobId: %v - %v. See: %v\n",
			utils.FileLine(),
			ctx.Jid(),
			"error fetching stop phone numbers from the database",
			err)

		loglines = jobLog(loglines, "error fetching stop phone numbers from the database")
		loglines = jobLog(loglines, "Job: %v ending due to an error", ctx.Jid())
		wrtJobLog(loglines, psdid, ctx.Jid(), cfg.WorkerCurEnv) // Write to the Promotion Send's log
		jberr = fmt.Errorf("error fetching stop phone numbers from the database")
		return jberr
	}
	if err == mgo.ErrNotFound {
		// no stop phone numbers found - possible error
		appLog("WARN: %v - JobId: %v - %v. See: %v\n",
			utils.FileLine(),
			ctx.Jid(),
			"no stop phone numbers fetched from the database - possible error condition",
			err)

		loglines = jobLog(loglines, "no stop phone numbers fetched from the database - possible error condition - continue processing")
	}

	// Iterate through the stop phone numbers and add to the cache
	errCtr := 0
	for _, elm := range mapCacheElms {
		err = cache.Set(fmt.Sprintf("%v", elm["phonetwilioformat"]), byteOne)
		if err != nil {
			// error setting a cache value
			appLog("ERROR: %v - error setting a cache value: %v. See: %v\n",
				utils.FileLine(),
				elm["phonetwilioformat"],
				err)

			errCtr = errCtr + 1
		}

		// Too many errors?
		if errCtr > 10 {
			// too many errors encountered constructing the cache
			appLog("ERROR: %v - JobId: %v - %v. See: %v\n",
				utils.FileLine(),
				ctx.Jid(),
				"too many errors encountered constructing the cache",
				err)

			loglines = jobLog(loglines, "too many errors encountered constructing the cache")
			loglines = jobLog(loglines, "Job: %v ending due to an error", ctx.Jid())
			wrtJobLog(loglines, psdid, ctx.Jid(), cfg.WorkerCurEnv) // Write to the Promotion Send's log
			jberr = fmt.Errorf("too many errors encountered constructing the cache")
			return jberr
		}
	}

	appLog("INFO: %v - JobId: %v - %v %v\n",
		utils.FileLine(),
		ctx.Jid(),
		"creating a cache of stop phone numbers with a number of entries:",
		cache.Len())

	// TODO: Future code - make sure there are no other "bv-job-worker-smsmsgs" jobs are currently running for this account

	// Iterate through the message data
	ntrvlCtr = 0
	for i := 0; i < limit; i++ {
		// SHOULD JOB BE HALTED? CHECK RELATED WEB SERVICE
		// Check the promotionsend id - halt job?
		if ntrvlCtr > cfg.WorkerSMSHaltJobInterval {
			// Exceeded the interval threshold
			// Check if job should be halted
			if stopWorker([]string{cfg.WorkerCurEnv, psdid, ctx.Jid()}) {
				appLog("HALT: %v - halting job: %v before sending phone #%v - %v\n",
					utils.FileLine(),
					ctx.Jid(),
					i,
					drtv.Data[i]["phonenumber"])
				loglines = jobLog(loglines, "halting job: %v before sending phone #%v - %v",
					ctx.Jid(),
					i,
					drtv.Data[i]["phonenumber"])

				// Indicate that the promotion broadcast has been halted
				err = setPSendHalted(
					psdid,
					fmt.Sprintf(
						"halting job: %v before sending phone #%v - %v",
						ctx.Jid(),
						i,
						drtv.Data[i]["phonenumber"],
					),
				)
				if err != nil {
					// error flagging a promotion send as halted
					log.Printf("ERROR: %v - error flagging a promotion %v send as halted. See: %v\n",
						utils.FileLine(),
						psdid,
						err)
				}

				jberr = fmt.Errorf("job halted before sending phone #%v - %v", i, drtv.Data[i]["phonenumber"])
				break
			}

			ntrvlCtr = 0
			continue
		}
		ntrvlCtr++

		// Log progress at certain intervals
		if i%cfg.WorkerProgressInterval == 0 {
			appLog("INFO: %v - job: %v currently processing %v of %v messages\n",
				utils.FileLine(),
				ctx.Jid(),
				i,
				limit)
		}

		// Validate the message data
		if !hlprIsPhone(drtv.Data[i]["phonenumber"]) {
			// invalid phone number
			appLog("WARN: %v - JobId: %v - %v. See: %v\n",
				utils.FileLine(),
				ctx.Jid(),
				"invalid phone number",
				drtv.Data[i]["phonenumber"])

			loglines = jobLog(loglines,
				fmt.Sprintf("invalid phone number: %v for customer #%v",
					drtv.Data[i]["phonenumber"], i))

			continue // skip to next customer
		}
		phn = hlprTransPhone(drtv.Data[i]["phonenumber"])
		if len(drtv.Message) == 0 {
			// message is missing
			appLog("WARN: %v - JobId: %v. See: %v\n",
				utils.FileLine(),
				ctx.Jid(),
				"missing message copy/text")

			loglines = jobLog(loglines,
				fmt.Sprintf("missing message copy/text for phone number: %v - customer #%v",
					drtv.Data[i]["phonenumber"], i))

			continue // skip to next customer
		}
		msg = drtv.Message
		if len(drtv.Data[i]["firstname"]) != 0 && !flgGenPromo {
			// firstname has been specified and NOT an xPathway generic message, add "salutation" to the text message
			msg = fmt.Sprintf("Hey %v! %v", drtv.Data[i]["firstname"], msg)
		}

		//* Skip phone numbers on the stop phonenumber list
		rslt, err := cache.Get(fmt.Sprintf("+1%v", phn))
		if err == nil && rslt != nil {
			// phone number is on the stoplist - skip
			loglines = jobLog(loglines, fmt.Sprintf(
				"Phone number: %v is on the xPathway stop list. Skipping", drtv.Data[i]["phonenumber"]))

			// Log this event to the main web application
			go logSkipStopPhoneNumber(drtv.Data[i]["phonenumber"])

			continue // skip number
		}
		if err != nil && err != bigcache.ErrEntryNotFound {
			// some error occurred finding a stop phone number from the cache that is not "entry not found"
			appLog("ERROR: %v - some error occurred finding a stop phone number: %v from the cache. See: %v\n",
				utils.FileLine(),
				fmt.Sprintf("+1%v", phn),
				err)

			loglines = jobLog(loglines, fmt.Sprintf(
				"Error checking the stop phone number cache for number: %v. Skipping", drtv.Data[i]["phonenumber"]))

			continue // skip number
		}

		//* Generate the text specific (unique at the promotion/phone number/text level) shortlink
		shrtlnk = newShortLink(cfg.WorkerCurEnv)
		err = shrtlnk.saveToDB()
		if err != nil {
			// error occurred saving the shortlink to the database - log error but keep processing
			appLog("ERROR: %v - error occurred saving the shortlink: %v to the database. See: %v\n",
				utils.FileLine(),
				shrtlnk.ShortLink,
				err)
		}

		//* Replace the text message's placeholder characters with the generated shortlink
		msg = strings.Replace(msg, cntThreePipesPlaceholder, shrtlnk.ShortLink, 1)

		// Construct the parameters to be sent via the HTTP POST
		urlVals := url.Values{}

		//* Override the Twilio from number if specified
		urlVals.Set("From", cfg.WorkerTwilioNumber)
		if rgxPhoneTwilioFormat.MatchString(drtv.TwilioOverridePhoneNum) {
			appLog("DEBUG: %v - override telephone number is: %v\n", utils.FileLine(), drtv.TwilioOverridePhoneNum)
			// a valid formatted Twilio override number exists -> use instead of the default
			urlVals.Set("From", drtv.TwilioOverridePhoneNum)
		}
		urlVals.Set("To", fmt.Sprintf("+1%v", phn))
		urlVals.Set("Body", msg)
		if len(drtv.MediaURL) != 0 {
			// Media URL is included - add to Twilio request
			urlVals.Set("MediaUrl", drtv.MediaURL)
		}

		reqURL = twilioConstructMessageURL
		// Send Twilio request to a test server (stub out Twilio)?
		if cfg.WorkerStubTwilio {
			// * NOTE: DEPRECATED - USE THE FAUX TWILIO NUMBER FOR STUB TWILIO CALLS (SEE TWILIO DOCUMENTATION)
			// yes, stubbing the Twilio API
			reqURL = cfg.WorkerStubTwilioTestURL

			appLog("INFO: %v - JobId: %v - trigger text web service message for phone number: %v - stubbing Twilio API calls to a test (faux) server: %v\n",
				utils.FileLine(),
				ctx.Jid(),
				phn,
				cfg.WorkerStubTwilioTestURL)

			loglines = jobLog(loglines,
				fmt.Sprintf("note: making fake Twilio request to: %v - real text message should not occur",
					cfg.WorkerStubTwilioTestURL))
		}

		// Configure an HTTP POST
		spew.Dump(urlVals)
		resp, err = http.PostForm(reqURL, urlVals)
		if err != nil {
			appLog("ERROR: %v - JobId: %v - %v. See: %v\n",
				utils.FileLine(),
				ctx.Jid(),
				"error posting text message to Twilio",
				err)

			loglines = jobLog(loglines,
				fmt.Sprintf("error posting text message: %v to Twilio - %v",
					drtv.Data[i]["phonenumber"], err))

			continue
		}
		defer resp.Body.Close()

		// Read Twilio's response body
		bBytes, err = ioutil.ReadAll(resp.Body)
		if err != nil {
			appLog("ERROR: %v - JobId: %v - %v. See: %v\n",
				utils.FileLine(),
				ctx.Jid(),
				"error reading Twilio's response",
				err)

			loglines = jobLog(loglines,
				fmt.Sprintf("unknown Twilio response for number: %v - %v",
					drtv.Data[i]["phonenumber"], err))

			time.Sleep(dur) // Wait before sending next text message
			continue
		}

		appLog("INFO: %v - JobId: %v - %v. Twilio message sent for broadcast: %v with status: %v and response: %v\n",
			utils.FileLine(),
			ctx.Jid(),
			drtv.Data[i]["phonenumber"],
			psdid,
			resp.StatusCode,
			string(bBytes))

		loglines = jobLog(loglines, fmt.Sprintf(
			"Twilio message sent to: %v (shortlink: %v) with status: %v",
			drtv.Data[i]["phonenumber"],
			shrtlnk.ShortLink,
			resp.StatusCode))

		// Parse the Twilio call json data into a Go object
		tMap := make(map[string]interface{})
		tMap["from"] = urlVals.Get("From")
		tMap["to"] = urlVals.Get("To")
		tMap["body"] = urlVals.Get("Body")
		tMap["mediaurl"] = urlVals.Get("MediaUrl")

		// Create an smsmessage object
		sms := newSMSMessage(
			bson.ObjectIdHex(drtv.PromoSendID),
			tMap,
			resp.StatusCode,
			resp.Status,
			string(bBytes),
			shrtlnk.ShortLink,
			shrtlnk.ShortCode,
			ctx.Jid())

		// Flag the sms object indicating if the underlying promotion is an xPathway generic promotion
		sms.XPathIsGenericPromo = flgGenPromo

		// Persist the smsmessage to the database
		err = sms.persistSMSMsg(cfg.WorkerCurEnv)
		if err != nil {
			// error occurred saving an smsmessage object to the database
			appLog("ERROR: %v - JobId: %v - %v - %v. See: %v\n",
				utils.FileLine(),
				ctx.Jid(),
				"error occurred saving an smsmessage object",
				sms.ID.Hex(),
				err)
		}

		time.Sleep(dur) // Wait before next text message
	}

	// Push a Faktory job that generates QR codes for sms messages sent by this processing job: ctx.Jid
	job = faktory.NewJob(cmn.CntWorkerNameQRCodeGen, ctx.Jid())
	job.ReserveFor = cfg.WorkerJobTimeoutSec // Assign a timeout
	job.Queue = cmn.CntWorkerNameQRCodeGen   // Assing to a queue
	job.Retry = -1                           // Failed jobs should are not retried and moved to the 'Dead' Faktory tab
	err = cmnWrkr.FakClient.Push(job)
	if err != nil {
		// error pushing a QR encoding worker job on a queue
		appLog("ERROR: %v - JobId: %v - %v. See: %v\n",
			utils.FileLine(),
			ctx.Jid(),
			"error pushing a QR encoding worker job on a queue",
			err)
	} else {
		appLog("INFO: %v - JobId: %v - job: %v (%v) pushed to create QR codes for sms messages sent by this executing job: %v\n",
			utils.FileLine(),
			ctx.Jid(),
			cmn.CntWorkerNameQRCodeGen,
			job.Jid,
			ctx.Jid())
	}

	// Push a Faktory job that takes sms data snapshots for messages sent by this processing job: ctx.Jid
	job = faktory.NewJob(cmn.CntWorkerNameSMSSnaps, ctx.Jid())
	job.ReserveFor = cfg.WorkerJobTimeoutSec // Assign a timeout
	job.Queue = cmn.CntWorkerNameSMSSnaps    // Assing to a queue
	job.Retry = -1                           // Failed jobs should are not retried and moved to the 'Dead' Faktory tab
	err = cmnWrkr.FakClient.Push(job)
	if err != nil {
		// error pushing a an SMS message snapshot job
		appLog("ERROR: %v - JobId: %v - %v. See: %v\n",
			utils.FileLine(),
			ctx.Jid(),
			"error pushing a an SMS message snapshot job",
			err)
	} else {
		appLog("INFO: %v - JobId: %v - job: %v (%v) pushed to create sms data snapshots for messages sent by this executing job: %v\n",
			utils.FileLine(),
			ctx.Jid(),
			cmn.CntWorkerNameSMSSnaps,
			job.Jid,
			ctx.Jid())
	}

	// Write the log
	loglines = jobLog(loglines, "End of job: %v", ctx.Jid())
	wrtJobLog(loglines, psdid, ctx.Jid(), cfg.WorkerCurEnv) // Write to the Promotion Send's log

	return jberr
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

	// Get the URL for the attached Heroku Redis add-on
	redisURL := os.Getenv("REDIS_URL")
	if len(redisURL) == 0 {
		// error fetching the attached Heroku Redis add-on URL
		log.Printf("FATAL: %v - error fetching the attached Heroku Redis add-on URL\n", utils.FileLine())
		os.Exit(1)
	}

	// Assign the Redis URL to the configuration object
	cfg.WorkerRedisURL = redisURL

	// Append a slash if needed to the base shorlink urls
	cfg.ShortLinkBaseURL = hlprConfirmSlash(cfg.ShortLinkBaseURL)
	cfg.ShortLinkBaseURLDev = hlprConfirmSlash(cfg.ShortLinkBaseURLDev)
	cfg.ShortLinkBaseURLStg = hlprConfirmSlash(cfg.ShortLinkBaseURLStg)

	// Are we stubbing out the Twilio URL (posting SMS requests to a faux server)?
	if cfg.WorkerStubTwilio {
		// yes, stubbing the Twilio API
		appLog("INFO: %v - stubbing Twilio API calls to a test (faux) server: %v\n",
			utils.FileLine(),
			cfg.WorkerStubTwilioTestURL)
	}

	// Set a time location
	timePT, err = time.LoadLocation(timeLocStr)
	if err != nil {
		// error setting the timezone location
		appLog("FATAL: %v - error fetching and parsing environment variable data. See: %v\n", utils.FileLine(), err)
		os.Exit(1)
	}

	// ***********************************************************************************

	// Dial the MongoDB database
	mgoURL = fmt.Sprintf(cfg.WorkerDBURL, cfg.WorkerDBUser, cfg.WorkerDBPassword)
	appLog("INFO: %v - connecting to the MongoDB database\n", utils.FileLine())
	mgoSession, err = mgo.Dial(mgoURL)
	if err != nil {
		// error dialing the MongoDB database
		appLog("FATAL: %v - fatal error dialing the hosted MongoDB database. See: %v\n", utils.FileLine(), err)
		os.Exit(1)
	}
	appLog("INFO: %v - successfully connected to the hosted MongoDB database\n", utils.FileLine())

	// Assign production MongoDB variables
	mgoDB = mgoSession.DB(cfg.WorkerDBName)
	mgoMgoCollSMSMessages = mgoDB.C("smsmessages")           //
	mgoCollWorkerJobLog = mgoDB.C("workerjoblog")            // collection holding log data/lines for a worker job run
	mgoCollWorkerJobData = mgoDB.C("workerjobdata")          // collection holding the data to be processed by individual worker jobs
	mgoMgoCollPromoBroadcasts = mgoDB.C("promobroadcasts")   //
	mgoCollShortLinkStore = mgoDB.C("shortlinkstore")        // collection storing generated shortlinks
	mgoMgoCollPromoCustTexts = mgoDB.C("promocustomertexts") // collection storing promotion/customer/text instances or combinations
	mgoCollStopPhoneList = mgoDB.C("stopphonelist")          // collection housing phone numbers that do NOT get text messages

	// Fire goroutine used to execute cron jobs
	go schedJobs()

	// Configure the Twilio URL
	twilioConstructMessageURL = twilioURLElement1 + cfg.WorkerTwilioAccountSID + twilioURLElement2 + cfg.WorkerTwilioAuthToken +
		twilioURLElement3 + cfg.WorkerTwilioAccountSID + twilioURLElement4

	// Close the MongoDB session at the end of processing
	defer mgoSession.Close()

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

	// Create a new worker object
	mgr := worker.NewManager()

	// Register the PromoSendSMSMessage worker
	mgr.Register(cmn.CntWorkerNameSendSMSMessages, wkrFirePromoSendSMSMsgs)

	// use up to N goroutines
	mgr.Concurrency = 5

	// pull jobs from these queues
	mgr.ProcessStrictPriorityQueues("critical", cmn.CntWorkerNameSendSMSMessages)

	// Start processing jobs
	go mgr.Run()

	// Start up a gin webserver
	port := os.Getenv("PORT")
	if port == "" {
		log.Fatal("FATAL: $PORT must be set for Heroku processing\n")
	}
	r := gin.Default()

	// Configure a status route
	r.GET("/status", cmnWrkr.HndlrStatus)

	// Start the "wrapping" web server
	log.Printf("INFO: %v - starting the 'wrapping' web server...\n", utils.FileLine())
	r.Run(fmt.Sprintf(":%v", port))
}
