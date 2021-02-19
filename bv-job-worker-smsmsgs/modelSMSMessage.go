package main

import (
	"fmt"
	"strings"
	"time"

	"github.com/danoand/utils"

	bson "github.com/globalsign/mgo/bson"
)

// smsmessage models sms messages sent out when a promotion is broadcast
type smsmessage struct {
	ID                   bson.ObjectId          `bson:"_id" json:"docid"`
	PromotionSendID      bson.ObjectId          `bson:"promotionsendid" json:"promotionsendid"`
	Time                 time.Time              `bson:"time" json:"time"`
	TimeDisplay          string                 `bson:"timedisplay" json:"timedisplay"`
	TwilioStatus         int                    `bson:"twiliostatus" json:"twiliostatus"`
	TwilioStatusMsg      string                 `bson:"twiliostatusmsg" json:"twiliostatusmsg"`
	TwilioResponse       string                 `bson:"twilioresponse" json:"twilioresponse"`
	MsgData              map[string]interface{} `bson:"msgdata" json:"msgdata"`
	ShortLink            string                 `bson:"shortlink" json:"shortlink"`
	ShortCode            string                 `bson:"shortcode" json:"shortcode"`
	ShortCodeUpperCase   string                 `bson:"shortcodeuppercase" json:"shortcodeuppercase"`
	MobilePhoneTo        string                 `bson:"mobilephoneto" json:"mobilephoneto"`
	MobilePhoneFrom      string                 `bson:"mobilephonefrom" json:"mobilephonefrom"`
	IsPromoExpiredString string                 `bson:"ispromoexpiredstring" json:"ispromoexpiredstring"`               // utility field used for display in the front end
	IsRedeemed           bool                   `bson:"isredeemed" json:"isredeemed"`                                   // indicates if this SMS Message has been redeemed
	JobID                string                 `bson:"jobid" json:"jobid"`                                             // Job id of the job that sent/fired the sms message
	XPathIsGenericPromo  bool                   `bson:"promogenxpathisgenericpromo" json:"promogenxpathisgenericpromo"` // indicates that the underlying promo is an xPathway generic promotion
}

// newSMSMessage is a function that creates a new smsmessage object
func newSMSMessage(
	pid bson.ObjectId,
	data map[string]interface{},
	status int,
	statusmsg string,
	resp string,
	link string,
	code string,
	jobid string,
) *smsmessage {

	var msg smsmessage

	// Populate the smsmessage object
	msg.ID = bson.NewObjectId()
	msg.MsgData = data
	msg.PromotionSendID = pid
	msg.Time = time.Now().In(timePT)
	msg.TwilioStatus = status
	msg.TwilioStatusMsg = statusmsg
	msg.TwilioResponse = resp
	msg.ShortLink = link
	msg.ShortCode = code
	msg.ShortCodeUpperCase = strings.ToUpper(code)
	msg.MobilePhoneFrom = hlprStripPlusOne(fmt.Sprintf("%v", data["from"]))
	msg.MobilePhoneTo = hlprStripPlusOne(fmt.Sprintf("%v", data["to"]))
	msg.TimeDisplay = hlprDispDateTime(msg.Time)
	msg.JobID = jobid

	return &msg
}

// persistSMSMsg is a method on smsmessage that saves the object into the database
func (s *smsmessage) persistSMSMsg(env string) error {
	var err error

	// Save the object to the appropriate database
	err = mgoMgoCollSMSMessages.Insert(s)
	if err != nil {
		appLog("ERROR: %v - error inserting an sms message object to the %v database. See: %v\n",
			utils.FileLine(),
			env,
			err)
	}

	return err
}

// countSMSSends counts the number of status 201 SMS messages for a particular promotion send
func countSMSSends(pid bson.ObjectId) (int, error) {
	var err error
	var num int

	num, err = mgoMgoCollSMSMessages.Find(bson.M{"promotionsendid": pid, "twiliostatus": 201}).Count()
	if err != nil {
		// error occurred counting SMS message objects/documents
		appLog("ERROR: %v - error occurred counting SMS message objects/documents. See: %v\n", utils.FileLine(), err)
	}

	return num, err
}
