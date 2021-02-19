package main

import (
	"fmt"
	"log"
	"log/syslog"
	"os"
	"time"

	worker "github.com/contribsys/faktory_worker_go"
	"github.com/danoand/utils"
	"github.com/gin-gonic/gin"
	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	"github.com/kelseyhightower/envconfig"

	cmn "github.com/whopdan/wrkrcom"
)

var (
	err            error
	cfg            Specification
	pprlog         *syslog.Writer
	mgoErr         error
	mgoURL         string
	mgoSession     *mgo.Session
	mgoDB          *mgo.Database
	mgoQRURL       string
	mgoQRSession   *mgo.Session
	mgoQRDB        *mgo.Database
	mgoQRGridFiles *mgo.GridFS
	cmnWrkr        cmn.Domain // access common worker functions
)

// Specification defines an object to house environment variable values
type Specification struct {
	WorkerDBURL               string `required:"true"`
	WorkerDBName              string `required:"true"`
	WorkerDBUser              string `required:"true"`
	WorkerDBPassword          string `required:"true"`
	WorkerQRDBURL             string `default:"mongodb://%v:%v@ds131125-a0.mlab.com:31125/qrcode-img-db"` // db to store QR images as blobs
	WorkerQRDBName            string `default:"qrcode-img-db"`
	WorkerQRDBUser            string `default:"imgwriter"`
	WorkerQRDBPassword        string `required:"true"`
	WorkerCurEnv              string `required:"true"`                                                                                                                                                                                            // Current Heroku environment: development, staging, production
	WorkerDevEnv              string `default:"http://localhost:8080/redeematpos/%v"`                                                                                                                                                             // Dev environment URL to encode
	WorkerStgEnv              string `default:"http://staging.broadvibe.com/redeematpos/%v"`                                                                                                                                                      // Staging environment URL to encode
	WorkerProdEnv             string `default:"https://app.broadvibe.com/redeematpos/%v"`                                                                                                                                                         // Production environment URL to encode
	WorkerRedisURL            string `envconfig:"REDIS_URL"`                                                                                                                                                                                      // Heroku Redis instance URL
	EnvkeenioprojectidProd    string `default:"5b85776ec9e77c0001bccadf"`                                                                                                                                                                         // Keen.io API project
	EnvkeeniowritekeyProd     string `default:"13658509C1C72AA2BD4BF18CE1CDFF1BF549FC86F0D883F5F22B488C08563E81E6763782BE51DCCF68A21E956A5C5BCC3966FBB7BA297F0DD233897156AB590C8D7154BA08E07D8B9B6BA18A48FF8C3701B2BA5F8DA43E3BBEF50D1A6A3CD315"` // Keen.io API Key
	EnvkeenioprojectidNonProd string `default:"59d54135c9e77c0001155a2f"`
	EnvkeeniowritekeyNonProd  string `default:"D37F7E85923C4592B0AC5F4068A2FB57A2184FB50FA056E0B8939890E4FE196205A90264447A6C4DB4712DA76EBAE9E7E212E1602EAF3FC6F08F32EAE615D6380711599D5EB97B625DCD94D0A0A7956C1AB6A27387AC2352175232E27ED02853"`
	FaktoryURL                string `envconfig:"FAKTORY_URL"`
}

// SMSMsg defines SMS Message data to be queried from the application database
type SMSMsg struct {
	ID        bson.ObjectId `bson:"_id"`
	ShortCode string        `json:"shorcode"`
	PsendID   bson.ObjectId `bson:"promotionsendid"`
}

// WorkerSMSSnapData models the data to be stored to the database and transmitted to the event service (Keen.io)
type WorkerSMSSnapData struct {
	ID          bson.ObjectId
	Context     map[string]string
	Environment string
	Content     map[string]interface{}
}

// WorkerSMSSnapDataContent models the content of the event logged via the WorkerSMSSnapData model
type WorkerSMSSnapDataContent struct {
	Time      time.Time
	TimeStamp string
	IDSMS     string
	IDPsend   string
	IDPromo   string
	IDProd    string
}

// wkrGenSMSSnapShot is a Faktory "job" that:
//   1. Takes a snapshot of the SMS message and related data and stores it as an event in the environment's database
//   2. Logs the "event" data structure to Keen.io so metrics can ge reported on downstream
// the job takes this parameter
//   * Job Id (of the job that sent the underlying sms message)
func wkrGenSMSSnapShot(ctx worker.Context, args ...interface{}) error {
	var (
		err        error
		ok         bool
		ctr        = 0 // "counts" the number of successfully processed iterations (sms messages)
		env, jid   string
		msgs       []smsmessage
		psend      promotionSend
		tpid       bson.ObjectId                  // temp document id value
		tmpProdMap map[string]interface{}         // temp product card snap object
		eventSnap  = make(map[string]interface{}) // the snapshot data content to be saved to the DB and Keen.io
	)

	// Grab the job id and use for logging messages
	jid = ctx.Jid()
	// Grab the environment
	env = cfg.WorkerCurEnv

	log.Printf("INFO: Job Id: %v - %v - starting job\n", jid, utils.FileLine())

	// Validate the job parameters
	if len(args) == 0 {
		// invalid job parameters - not enough parameters
		log.Printf("ERROR: Job Id: %v - %v - invalid number of job parameters: %v\n",
			ctx.Jid(),
			utils.FileLine(),
			len(args))

		return fmt.Errorf("invalid number of job parameters: %v - need the environment type and shortcode", len(args))
	}

	// Are the required arguments strings?
	jid, ok = args[0].(string)
	if !ok || len(jid) == 0 {
		// unexpected parameter type or missing parameter
		log.Printf("ERROR: Job Id: %v - %v - job id parameter is an unexpected type or is missing: %T\n",
			ctx.Jid(),
			utils.FileLine(),
			args[0])

		return fmt.Errorf("document parameter is an unexpected type or is missing")
	}

	// Fetch the SMS Messages that have been sent from the given Faktory jobid
	err = mgoDB.C("smsmessages").Find(bson.M{"jobid": jid}).All(&msgs)
	if err != nil {
		// error occurred querying for sms messages with the given jobid
		log.Printf("ERROR: Job Id: %v - %v - error occurred querying for sms messages with the given jobid: %v. See: %v\n",
			ctx.Jid(),
			utils.FileLine(),
			jid,
			err)

		return fmt.Errorf("error: %v", err)
	}

	// Iterate through the SMS messages
	log.Printf("INFO: Job Id: %v - %v - iterating through %v SMS Messages (originally sent via job: %v)\n",
		ctx.Jid(),
		utils.FileLine(),
		len(msgs),
		jid)

	// Fetch the SMS Messages that have been sent from the given Faktory jobid
	err = mgoDB.C("smsmessages").Find(bson.M{"jobid": jid}).All(&msgs)
	if err != nil {
		// error occurred querying for sms messages with the given jobid
		log.Printf("ERROR: Job Id: %v - %v - error occurred querying for sms messages associated with the given jobid: %v. See: %v\n",
			ctx.Jid(),
			utils.FileLine(),
			jid,
			err)

		return fmt.Errorf("error: %v", err)
	}

	// Iterate through the SMS messages
	log.Printf("INFO: Job Id: %v - %v - iterating through %v SMS Messages (originally sent via job: %v)\n",
		ctx.Jid(),
		utils.FileLine(),
		jid,
		len(msgs))

	for _, val := range msgs {

		// Fetch the promotion send (broadcast)
		err = mgoDB.C(cntMgoCollPSend).FindId(val.PromotionSendID).One(&psend)
		if err != nil {
			// error finding a promotion broadcast document
			msg := fmt.Sprintf("error finding a promotion broadcast document: %v in environment: %v.",
				val.PromotionSendID.Hex(),
				env)
			log.Printf("ERROR: Job #%v - %v - %v See: %v\n",
				jid,
				utils.FileLine(),
				msg,
				err)
			continue
		}

		// Construct the data object to be saved
		eventSnap["id_ownerid"] = psend.OwnerID
		eventSnap["ownertype"] = psend.OwnerType
		eventSnap["id_smsmessage"] = val.ID.Hex()
		eventSnap["id_promosend"] = psend.ID.Hex()
		eventSnap["id_promo"] = psend.PromoID.Hex()
		eventSnap["promotype"] = psend.PromoClass
		eventSnap["id_prod"] = psend.ProductID.Hex()
		eventSnap["snap_smsmessage"] = val
		eventSnap["snap_promosend"] = psend

		// Fetch the promotion document
		tmpMap := make(map[string]interface{})
		switch psend.PromoClass {

		// CASE: "snap" a general promotion
		case cntPromoTypeGeneral:
			err = mgoDB.C(cntMgoCollPromoGen).FindId(psend.PromoID).One(&tmpMap)
			if err != nil {
				// error fetching a general promotion
				msg := fmt.Sprintf("error fetching a general promotion: %v. See: %v", psend.PromoID.Hex(), err)
				log.Printf("ERROR: Job #%v - %v - %v\n",
					jid,
					utils.FileLine(),
					msg)
			}

		// CASE: "snap" a standard promotion
		case cntPromoTypeStandard:
			err = mgoDB.C(cntMgoCollPromo).FindId(psend.PromoID).One(&tmpMap)
			if err != nil {
				// error fetching a standard promotion
				msg := fmt.Sprintf("error fetching a standard promotion: %v. See: %v", psend.PromoID.Hex(), err)
				log.Printf("ERROR: Job #%v - %v - %v\n",
					jid,
					utils.FileLine(),
					msg)
			}

		// DEFAULT: unexpected promotion type - error
		default:
			msg := fmt.Sprintf("unexpected promotion type %v.", psend.PromoClass)
			log.Printf("ERROR: Job #%v - %v - %v\n",
				jid,
				utils.FileLine(),
				msg)
		}

		// Assign the promotion data
		eventSnap["snap_promo"] = tmpMap

		// First iteration or encountering a new promotion send object? Snap the underlying product data
		if tpid != psend.ID {
			// First iteration or processing a different promotion broadcast than the last iteration (rare)
			tmpProdMap = make(map[string]interface{})

			if psend.PromoClass == cntPromoTypeStandard {
				// Standard promotion - should have an underlying product
				err = mgoDB.C(cntMgoCollProdCards).FindId(psend.ProductID).One(&tmpProdMap)
				if err != nil {
					// error fetching a product card
					msg := fmt.Sprintf(
						"error fetching an underlying promotion product: %v for a %v promotion: %v. See: %v",
						psend.ProductID.Hex(),
						psend.PromoClass,
						psend.PromoID.Hex(),
						err)
					log.Printf("ERROR: Job #%v - %v - %v\n",
						jid,
						utils.FileLine(),
						msg)

					tmpProdMap["msg"] = "no product card data found"
				}
			}
		}

		// Assign the promotion data
		eventSnap["snap_prod"] = tmpProdMap

		// Log the event data
		logMe(
			mgoDB,
			env,
			psend.OwnerID.Hex(),
			string(psend.OwnerType),
			"not fetched",
			cntEventSendSMSMessage,
			eventSnap,
		)

		tpid = psend.ID // set temp document id to the current broadcast id - for next iteration
		ctr++
	}

	log.Printf("INFO: Job Id: %v - %v - successfully 'snapped' %v of %v sms messages\n",
		ctx.Jid(),
		utils.FileLine(),
		ctr,
		len(msgs))

	log.Printf("INFO: Job Id: %v - %v - ending job\n", jid, utils.FileLine())

	return nil
}

func main() {
	// Grab environment variables
	fmt.Printf("INFO: %v - fetching local environment variables\n", utils.FileLine())
	err = envconfig.Process("bvapp", &cfg)
	if err != nil {
		// error fetching and parsing environment variable data
		fmt.Printf("FATAL: %v - error fetching and parsing environment variable data. See: %v\n", utils.FileLine(), err)
		os.Exit(1)
	}

	// Grab the port environment variable for Heroku processing
	port := os.Getenv("PORT")
	if port == "" {
		log.Fatal("FATAL: $PORT must be set for Heroku processing\n")
	}

	r := gin.Default()

	// ***********************************************************************************
	// Connect to the QR image Database
	log.Printf("INIT: %v - Connecting to hosted MongoDB QR image database\n", utils.FileLine())
	mgoQRURL = fmt.Sprintf(cfg.WorkerQRDBURL, cfg.WorkerQRDBUser, cfg.WorkerQRDBPassword)
	mgoQRSession, err = mgo.Dial(mgoQRURL)
	if err != nil {
		log.Printf("FATAL: %v - fatal error dialing the hosted Mongodb database. See: %v\nTerminating\n",
			utils.FileLine(),
			err)
	}

	// Ping the Mongodb to verify the connection
	log.Printf("INIT: %v - Testing the MongoDB QR image database connection\n", utils.FileLine())
	if err := mgoQRSession.Ping(); err != nil {
		log.Printf("FATAL: %v - fatal error pinging the Mongodb server. See: %v.\nTerminating.",
			utils.FileLine(),
			err)
	}
	log.Printf("INIT: %v - Ping of the QR image Mongodb was successful\n", utils.FileLine())

	// Define references to the database and collections
	mgoQRDB = mgoQRSession.DB(cfg.WorkerQRDBName) //
	mgoQRGridFiles = mgoQRDB.GridFS("fs")         // GridFS for housing files used for SMS background jobs

	// ***********************************************************************************
	// Connect to the application MongoDB database
	log.Printf("INIT: %v - Connecting to hosted MongoDB database\n", utils.FileLine())
	mgoURL = fmt.Sprintf(cfg.WorkerDBURL, cfg.WorkerDBUser, cfg.WorkerDBPassword)
	mgoSession, err = mgo.Dial(mgoURL)
	if err != nil {
		log.Printf("FATAL: %v - fatal error dialing the hosted Mongodb database. See: %v\nTerminating\n",
			utils.FileLine(),
			err)
	}

	// Ping the Mongodb to verify the connection
	log.Printf("INIT: %v - Testing the MongoDB connection\n", utils.FileLine())
	if err := mgoSession.Ping(); err != nil {
		log.Printf("FATAL: %v - fatal error pinging the Mongodb server. See: %v.\nTerminating.",
			utils.FileLine(),
			err)
	}
	log.Printf("INIT: %v - Ping of the Mongodb was successful\n", utils.FileLine())

	// Define references to the database and collections
	mgoDB = mgoSession.DB(cfg.WorkerDBName)

	// Fire goroutine used to execute cron jobs
	go schedJobs()

	//* ***************************************
	//* Set up the Faktory worker
	//* ***************************************
	log.Printf("INFO: %v - setting up the Faktory worker job\n", utils.FileLine())
	mgr := worker.NewManager()

	// Construct the injected values (for the common handlers/functions)
	cmnWrkr, err = cmn.NewDomain()
	if err != nil {
		// error creating a Faktory client
		log.Printf("WARN: %v - error creating a Faktory client. See: %v\n", utils.FileLine(), err)
	}
	cmnWrkr.Mgo = mgoSession
	cmnWrkr.MgoDB = mgoDB

	// register job types and the function to execute them
	log.Printf("INFO: %v - registering Faktory job: bv-job-worker-smssnap-gen, bv-job-worker-smssnap-gen-dummy\n", utils.FileLine())
	mgr.Register(cmn.CntWorkerNameSMSSnaps, wkrGenSMSSnapShot) // register the worker function

	// use up to N goroutines to execute jobs
	mgr.Concurrency = 3

	// pull jobs from these queues, in this order of precedence
	mgr.ProcessStrictPriorityQueues(cmn.CntWorkerNameSMSSnaps)

	// Start processing jobs, this method does not return
	log.Printf("INFO: %v - starting the worker instance(s) and polling for work...\n", utils.FileLine())
	go mgr.Run()

	// Configure a status route
	r.GET("/status", cmnWrkr.HndlrStatus)

	// Start the "wrapping" web server
	log.Printf("INFO: %v - starting the 'wrapping' web server...\n", utils.FileLine())
	r.Run(fmt.Sprintf(":%v", port))
}
