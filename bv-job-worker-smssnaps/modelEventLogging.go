package main

import (
	"bytes"
	"fmt"
	"log"
	_ "log"
	"net/http"
	"time"

	"github.com/danoand/utils"

	valid "github.com/asaskevich/govalidator"
	mgo "github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
)

// eventContext houses the context of most user events including: environment, usertype, username, and user document id
type eventContext struct {
	Environment string `json:"environment"`
	UserDocID   string `json:"userdocumentid"`
	UserName    string `json:"username"`
	UserType    string `json:"usertype"`
}

// logEvent models an event that will be logged in the application database and Keen.io
type logEvent struct {
	ID          bson.ObjectId          `bson:"_id" json:"docid"`
	Time        time.Time              `json:"time"`
	Timestamp   string                 `json:"timestamp"`
	UserContext eventContext           `json:"usercontext"`
	Environment string                 `json:"environment"`
	Event       string                 `json:"event"`
	Data        map[string]interface{} `json:"data"`
}

// logMe logs an event into a MongoDB database
func logMe(tmpDB *mgo.Database, envmt, usrdocid, usrtype, usrname, event string, ev map[string]interface{}) {
	var (
		err, perr error
		evStr     string
		lev       logEvent
	)

	// Populate the logevent
	lev.ID = bson.NewObjectId()
	lev.Data = ev
	lev.Environment = envmt
	lev.Event = event
	timePT, _ := time.LoadLocation("America/Los_Angeles")
	lev.Time = time.Now().In(timePT)
	lev.Timestamp = lev.Time.Format(time.RFC3339)
	lev.UserContext.Environment = envmt
	lev.UserContext.UserDocID = usrdocid
	lev.UserContext.UserName = usrname
	lev.UserContext.UserType = usrtype

	// Store the event in the MongoDB database
	err = tmpDB.C(cntMgoCollEvents).Insert(lev)
	if err != nil {
		log.Printf("ERROR: %v - error occurred logging an event to the MongoDB database.  See: %v\n", utils.FileLine(), err)

		// Attempt to write the log data to system log
		evStr, perr = valid.ToJSON(lev)
		if perr == nil {
			// Error writing an event to the MongoDB; write the error (as json) to the log
			log.Printf("LOGEVENT: Event: [%v]\n", evStr)
			return
		}

		// An error occurred generating the event as json; write a generic error to the log
		log.Printf("ERROR: %v - error occurred translating an event to a json string (intended to be written to the log). See: %v\n",
			utils.FileLine(),
			perr)
	}

	// Log event to Keen.io
	logMeKeenIO(envmt, lev)

	return
}

// logMeKeenIO logs an event to Keen.io via a webhook style POST
func logMeKeenIO(env string, lev logEvent) {
	var err error
	var tURL string
	var bBody []byte

	_, bBody, err = utils.ToJSON(lev)
	if err != nil {
		// error transforming the log event data into bytes of json
		log.Printf("ERROR: %v - error transforming the log event data into bytes of json. See: %v",
			utils.FileLine(),
			err)
		return
	}

	// Create a bytes io.reader for use in the http post downstream
	body := bytes.NewBuffer(bBody)

	// Construct the URL
	if env == cntAppEnvProd {
		//  Write a production environment metric to Keen.io
		tURL = fmt.Sprintf("https://api.keen.io/3.0/projects/%v/events/%v?api_key=%v",
			cfg.EnvkeenioprojectidProd,
			cntKeenIOCollectionName,
			cfg.EnvkeeniowritekeyProd)
	}
	if env != cntAppEnvProd {
		//  Write a production environment metric to Keen.io
		tURL = fmt.Sprintf("https://api.keen.io/3.0/projects/%v/events/%v?api_key=%v",
			cfg.EnvkeenioprojectidNonProd,
			cntKeenIOCollectionName,
			cfg.EnvkeeniowritekeyNonProd)
	}

	_, err = http.Post(tURL, "application/json", body)
	if err != nil {
		// error posting data to Keen.io
		log.Printf("ERROR: %v - error posting data to Keen.io. See: %v",
			utils.FileLine(),
			err)
	}

	return
}
