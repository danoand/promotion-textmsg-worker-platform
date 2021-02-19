package main

import (
	"context"
	"fmt"
	"image/png"
	"io"
	"log"
	"log/syslog"
	"os"
	"time"

	"go.mongodb.org/mongo-driver/mongo/gridfs"

	"github.com/boombuler/barcode"
	"github.com/boombuler/barcode/qr"
	worker "github.com/contribsys/faktory_worker_go"
	"github.com/danoand/utils"
	"github.com/gin-gonic/gin"
	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	"github.com/kelseyhightower/envconfig"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/whopdan/wrkrcom"
	cmn "github.com/whopdan/wrkrcom"
)

var (
	err            error
	cfg            Specification
	pprlog         *syslog.Writer
	appName        = "bv-job-worker-qrcode-gen"
	mgoErr         error
	mgoURL         string
	mgoSession     *mgo.Session
	mgoDB          *mgo.Database
	mgoQRURL       string
	mgoQRSession   *mgo.Session
	mgoQRDB        *mgo.Database
	clientQR       *mongo.Client
	mgoQRGridFiles *mgo.GridFS
	bucket         *gridfs.Bucket
	cmnWrkr        cmn.Domain // access common worker functions
)

// Specification defines an object to house environment variable values
type Specification struct {
	WorkerDBURL      string `required:"true"`
	WorkerDBName     string `required:"true"`
	WorkerDBUser     string `required:"true"`
	WorkerDBPassword string `required:"true"`
	WorkerQRDBURL    string `default:"mongodb+srv://imgwriter:Uk77LUG2X8ByWv88@ds131125-t9yur.mongodb.net/qrcode-img-db"` // db to store QR images as blobs
	WorkerQRDBName   string `default:"qrcode-img-db"`
	WorkerCurEnv     string `required:"true"`                                     // Current Heroku environment: development, staging, production
	WorkerDevEnvURL  string `default:"http://localhost:8080/redeematpos/"`        // Dev environment URL to encode
	WorkerStgEnvURL  string `default:"http://staging.broadvibe.com/redeematpos/"` // Staging environment URL to encode
	WorkerProdEnvURL string `default:"https://app.broadvibe.com/redeematpos/"`    // Production environment URL to encode
	WorkerRedisURL   string `envconfig:"REDIS_URL"`                               // Heroku Redis instance URL
}

// SMSMsg defines SMS Message data to be queried from the application database
type SMSMsg struct {
	ID        bson.ObjectId `bson:"_id"`
	ShortCode string        `json:"shorcode"`
	PsendID   bson.ObjectId `bson:"promotionsendid"`
}

// genEncodedURL constructs the URL to be encoded to a QR code
func genEncodedURL(shrtcode string) (string, error) {
	var encURL string

	// Determine the environment domain to be encoded
	switch cfg.WorkerCurEnv {

	// CASE: development
	case cntAppEnvDev:
		encURL = cfg.WorkerDevEnvURL + shrtcode

	// CASE: staging
	case cntAppEnvStage:
		encURL = cfg.WorkerStgEnvURL + shrtcode

	// CASE: production
	case cntAppEnvProd:
		encURL = cfg.WorkerProdEnvURL + shrtcode

	// ERROR case
	default:
		// error occurred - unhandled environment parameter
		return encURL, fmt.Errorf("error - unhandled environment parameter")
	}

	return encURL, nil
}

// createQRCodeImage creates a QR image and stores it as a MongoDB grid file
//   (NOTE: pulled out this code into its own function)
func createQRCodeImage(shrtcode string) error {
	var (
		err           error
		encURL, fname string
		qrCode        barcode.Barcode
	)

	// Validate the shortcode
	if len(shrtcode) == 0 {
		// missing shortcode
		return fmt.Errorf("missing shortcode")
	}

	// Generate the URL to be encoded
	encURL, err = genEncodedURL(shrtcode)
	if err != nil {
		// error generating the encode URL
		return fmt.Errorf("error generating the encode URL")
	}

	qrCode, err = qr.Encode(encURL, qr.M, qr.Auto)
	if err != nil {
		return fmt.Errorf("error generating a QR code object - %v", err)
	}

	// Scale the barcode to 200x200 pixels
	qrCode, err = barcode.Scale(qrCode, 200, 200)

	// Create a grid file to house the QR code
	fname = fmt.Sprintf("%v.png", shrtcode)

	// Set meta data for the grid file
	tmpMeta := bson.M{
		"shortcode":   shrtcode,
		"environment": cfg.WorkerCurEnv,
		"url":         encURL,
		"docid":       bson.NewObjectId().Hex()}

	// Define a pipe that writes the encoded QR code data stream to
	//   a reader that saves the QR encoded image as a MongoDB gridfs file
	pr, pw := io.Pipe()
	defer pr.Close()

	// Fire off a goroutine to write the encoded data in parallel to the gridfs saving code
	var encErr error
	go func() {
		// close the writer, so the reader knows there's no more data
		defer pw.Close()

		// Encode the qr code as a png file
		encErr = png.Encode(pw, qrCode)

	}()

	uploadOpts := options.GridFSUpload().SetMetadata(tmpMeta)
	fileID, err := bucket.UploadFromStream(fname, pr, uploadOpts)
	if err != nil {
		return fmt.Errorf("error uploading file: %v to the MongoDB database - %v", fname, err)
	}
	if encErr != nil {
		return fmt.Errorf("error encoding shortcode: %v as a QR image - %v", shrtcode, encErr)
	}

	fmt.Printf("new file created with ID %s", fileID)

	return nil
}

// genQRCode is a worker job that creates QR code images for SMS Messages that have been processed/sent
//   by Faktory job with a specific jobid
//  arg 1: jobid
func genQRCode(ctx worker.Context, args ...interface{}) error {
	var (
		ok        bool
		jid, scde string
		msgs      []SMSMsg
		ctr       = 0
	)

	log.Printf("INFO: Job Id: %v - %v - starting job: %v with job id: %v\n",
		ctx.Jid(),
		utils.FileLine(),
		ctx.JobType(),
		ctx.Jid())

	// Validate the job parameters
	if len(args) == 0 {
		// invalid job parameters - not enough parameters
		log.Printf("ERROR: Job Id: %v - %v - invalid number of job parameters: %v\n",
			ctx.Jid(),
			utils.FileLine(),
			len(args))

		return fmt.Errorf("invalid number of job parameters: %v - need shortcode", len(args))
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

	// Are we executing an ad hoc job to create a one off QR code?
	if jid == "adhoc" {
		// executing an adhoc job - check for a shortcode to encode
		if len(args) <= 1 {
			// shortcode parameter missing for the ad hoc job
			log.Printf("ERROR: Job Id: %v - %v - 'ad hoc' job request but shorcode parameter missing: %T\n",
				ctx.Jid(),
				utils.FileLine(),
				args[0])

			return fmt.Errorf("document parameter is an unexpected type or is missing")
		}

		scde, ok = args[1].(string)
		if !ok || len(scde) == 0 {
			// empty shortcode parameter
			log.Printf("ERROR: Job Id: %v - %v - job id parameter is an unexpected type or is missing: %T\n",
				ctx.Jid(),
				utils.FileLine(),
				args[0])

			return fmt.Errorf("document parameter is an unexpected type or is missing")
		}

		// execute this one off, ad hoc job with the passed shortcode
		err = createQRCodeImage(scde)
		if err != nil {
			// error generating the one off, ad hoc QR code file generation
			log.Printf("ERROR: Job Id: %v - %v - error generating the one off, ad hoc QR code file generation: %v. See: %v\n",
				ctx.Jid(),
				utils.FileLine(),
				jid,
				err)

			return fmt.Errorf("error: %v", err)
		}

		// End job successfully
		log.Printf("INFO: Job Id: %v - %v - completing job: %v encoded (%v of %v) shortcodes originally sent via job id: %v\n",
			ctx.Jid(),
			utils.FileLine(),
			ctx.JobType(),
			ctr,
			len(msgs),
			jid)
		return nil
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
	log.Printf("INFO: Job Id: %v - %v - iterating over %v SMS Message(s) (sent via job: %v) message redemption QR codes\n",
		ctx.Jid(),
		utils.FileLine(),
		len(msgs),
		jid)

	for _, val := range msgs {

		// Create the QR code
		err = createQRCodeImage(val.ShortCode)
		if err != nil {
			// error generating the URL to be coded
			log.Printf("ERROR: Job Id: %v - %v - error generating the URL to be coded: %v. See: %v\n",
				ctx.Jid(),
				utils.FileLine(),
				jid,
				err)

			return fmt.Errorf("error: %v", err)
		}

		ctr++
	}

	log.Printf("INFO: Job Id: %v - %v - completing job: %v encoded (%v of %v) shortcodes originally sent via job id: %v\n",
		ctx.Jid(),
		utils.FileLine(),
		ctx.JobType(),
		ctr,
		len(msgs),
		jid)
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
	clientQR, err := mongo.NewClient(options.Client().ApplyURI(cfg.WorkerQRDBURL))
	if err != nil {
		// error establishing a client to the QR database
		log.Printf("FATAL: %v - error establishing a client to the QR database. See: %v\nTerminating\n",
			utils.FileLine(),
			err)
		os.Exit(1)
	}
	// Connect to the database
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	err = clientQR.Connect(ctx)
	if err != nil {
		// error connecting to the MongoDB QR database
		log.Printf("FATAL: %v - error establishing a client to the QR database. See: %v\nTerminating\n",
			utils.FileLine(),
			err)
		os.Exit(1)
	}

	// Ping the Mongodb to verify the connection
	ctx, cancel = context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	err = clientQR.Ping(ctx, nil)
	if err != nil {
		// error pinging the QR MongoDB database
		log.Printf("FATAL: %v - error pinging the QR MongoDB database. See: %v\nTerminating\n",
			utils.FileLine(),
			err)
		os.Exit(1)
	}
	log.Printf("INIT: %v - Ping of the QR image Mongodb was successful\n", utils.FileLine())

	// Stand up the gridfs bucket reference
	bucket, err = gridfs.NewBucket(clientQR.Database(cfg.WorkerQRDBName))
	if err != nil {
		// error generating a bucket reference
		log.Printf("ERROR: %v - error generating a bucket reference. See: %v\n", utils.FileLine(), err)
	}

	// ***********************************************************************************
	// Connect to the application MongoDB database
	log.Printf("INIT: %v - Connecting to hosted MongoDB database\n", utils.FileLine())
	mgoURL = fmt.Sprintf(cfg.WorkerDBURL, cfg.WorkerDBUser, cfg.WorkerDBPassword)
	mgoSession, err = mgo.Dial(mgoURL)
	if err != nil {
		log.Printf("FATAL: %v - fatal error dialing the hosted Mongodb database. See: %v\nTerminating\n",
			utils.FileLine(),
			err)
		os.Exit(1)
	}

	// Ping the Mongodb to verify the connection
	log.Printf("INIT: %v - Testing the MongoDB connection\n", utils.FileLine())
	if err := mgoSession.Ping(); err != nil {
		log.Printf("FATAL: %v - fatal error pinging the Mongodb server. See: %v.\nTerminating.",
			utils.FileLine(),
			err)
		os.Exit(1)
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
		// error creating a common Faktory object
		log.Printf("ERROR: %v - error creating a common Faktory object. See: %v\n",
			utils.FileLine(),
			err)
		os.Exit(1)
	}
	cmnWrkr.Mgo = mgoSession
	cmnWrkr.MgoDB = mgoDB

	// register job types and the function to execute them
	log.Printf("INFO: %v - registering Faktory jobs: %v, cmnWrkr.DummyFunc\n",
		utils.FileLine(),
		wrkrcom.CntWorkerNameQRCodeGen)
	mgr.Register(cmn.CntWorkerNameQRCodeGen, genQRCode)
	mgr.Register("bv-job-worker-qrcode-gen-dummy", cmnWrkr.DummyFunc) // register a dummy function for status/debugging

	// use up to N goroutines to execute jobs
	mgr.Concurrency = 3

	// pull jobs from these queues, in this order of precedence
	mgr.ProcessStrictPriorityQueues(cmn.CntWorkerNameQRCodeGen)

	// Start processing jobs, this method does not return
	log.Printf("INFO: %v - starting the worker instance(s) and polling for work...\n", utils.FileLine())
	go mgr.Run()

	// Configure a simple Faktory status route
	r.GET("/status", cmnWrkr.HndlrStatus)

	// Start the "wrapping" web server
	log.Printf("INFO: %v - starting the 'wrapping' web server...\n", utils.FileLine())
	r.Run(fmt.Sprintf(":%v", port))
}
