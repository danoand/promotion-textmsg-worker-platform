// modelPromoCustomerLink.go models the associations of a promotion to a specific customer including unique, custom shortlinks
package main

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/danoand/utils"
	"github.com/globalsign/mgo/bson"
)

// List of possible shortlink characters
const charset = "0123456789" + "abcdefghijklmnopqrstuvwxyz" + "ABCDEFGHIJKLMNOPQRSTUVWXYZ"

// Random process seed used to randomly create a shortlink code
var seededRand = rand.New(rand.NewSource(time.Now().UnixNano()))

// StringWithCharset creates a random string of characters with a specified length using a specified set of characters
func StringWithCharset(length int, charset string) string {
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[seededRand.Intn(len(charset))]
	}
	return string(b)
}

// shortLinkObj models objects that "save" used shortlinks
type shortLinkObj struct {
	ID          bson.ObjectId `bson:"_id" json:"docid"`
	BaseURL     string        `bson:"baseurl" json:"baseurl"`
	DateTime    time.Time     `bson:"datetime" json:"datetime"`
	ShortCode   string        `bson:"shortcode" json:"shortcode"`
	ShortLink   string        `bson:"shortlink" json:"shortlink"`
	Environment string        `bson:"environment" json:"environment"`
}

// newShortLink creates a new shortlink
func newShortLink(env string) *shortLinkObj {
	var (
		retCode string
		link    shortLinkObj
	)

	// Create a random shortcode that serves as the "path" of the shortlink URL
	retCode = StringWithCharset(7, charset)

	link.ID = bson.NewObjectId()
	link.ShortCode = retCode
	link.DateTime = time.Now().In(timePT)
	link.Environment = env

	// Save the object to the appropriate database
	switch env {

	// CASE: development
	case cntAppEnvDev:
		link.BaseURL = cfg.ShortLinkBaseURLDev

	// CASE: staging
	case cntAppEnvStage:
		link.BaseURL = cfg.ShortLinkBaseURLStg

	// DEFAULT: insert into production
	default:
		link.BaseURL = cfg.ShortLinkBaseURL
	}

	link.ShortLink = link.BaseURL + retCode

	return &link
}

// saveToDB is a method on shortLinkObj that saves the object to the database
func (sl shortLinkObj) saveToDB() error {
	var (
		err      error
		nullTime time.Time
	)

	// Validate the object
	switch {

	// CASE: invalid document id?
	case !bson.IsObjectIdHex(sl.ID.Hex()):
		return fmt.Errorf("invalid document id")

	// CASE: missing object elements
	case len(sl.BaseURL) == 0 || len(sl.ShortCode) == 0 || len(sl.ShortLink) == 0:
		return fmt.Errorf("missing object element: base url, shortcode, or shortlink")

	// CASE: missing date/time stap
	case sl.DateTime == nullTime:
		// add a date/time stamp
		sl.DateTime = time.Now().In(timePT)
	}

	// Insert the object into the database
	err = mgoCollShortLinkStore.Insert(sl)
	if err != nil {
		return fmt.Errorf("error inserting a shortlink object to the database: %v", err)
	}

	return nil
}

// promoCustomerTextObj models an instance of a promotion sent to a customer via a promotion send (text)
type promoCustomerTextObj struct {
	ID          bson.ObjectId `bson:"_id" json:"docid"`               // document id
	SMSDocID    bson.ObjectId `bson:"smsdocid" json:"smsdocid"`       // SMS Message document id
	PsendDocID  bson.ObjectId `bson:"psenddocid" json:"psenddocid"`   // promotion broadcast (send) document id
	CustDocID   bson.ObjectId `bson:"custdocid" json:"custdocid"`     // end customer document id
	PhoneNumber string        `bson:"phonenumber" json:"phonenumber"` // end customer phone number
	ShortLink   string        `bson:"shortlink" json:"shortlink"`     // shortlink associated with the text
	ShortCode   string        `bson:"shortcode" json:"shortcode"`     // shortcode associated with the text
	TextSent    bool          `bson:"textsent" json:"textsent"`       // flag: text has been sent
	HasEvents   bool          `bson:"hasevents" json:"hasevents"`     // flag: has one or more events logged
	Environment string        `bson:"environment" json:"environment"` // environment
}

// newPromCustText creates a new promoCustomerTextObj object memorializing the promotion/customer/broadcast(text) combination
func newPromCustText(drtv *ProcessDirective, sms *smsmessage, callMap map[string]string) *promoCustomerTextObj {
	var obj promoCustomerTextObj

	obj.ID = bson.NewObjectId()
	obj.Environment = drtv.Environment
	obj.SMSDocID = sms.ID
	if bson.IsObjectIdHex(drtv.PromoSendID) {
		obj.PsendDocID = bson.ObjectIdHex(drtv.PromoSendID)
	}
	if bson.IsObjectIdHex(callMap["documentid"]) {
		// save the end customer document id if present and valid
		obj.CustDocID = bson.ObjectIdHex(callMap["documentid"])
	}
	obj.PhoneNumber = hlprTransPhone(callMap["phonenumber"])
	obj.ShortLink = sms.ShortLink
	obj.ShortCode = sms.ShortCode
	obj.TextSent = true

	return &obj
}

// saveToDB is a method on promoCustomerTextObj that saves the object to the database
func (pct promoCustomerTextObj) saveToDB(env string) error {
	var err error

	// Save the object to the appropriate database
	err = mgoMgoCollPromoCustTexts.Insert(pct)
	if err != nil {
		appLog("ERROR: %v - error inserting a promotion/customer/text object to the %v database. See: %v\n",
			utils.FileLine(),
			env,
			err)
	}

	return err
}

// promoCustomerTextEvent models an event logged for a promoCustomerTextObj
type promoCustomerTextEvent struct {
	ID                 bson.ObjectId          `bson:"_id" json:"docid"`                             // document id
	PromoCustTextDocID promoCustomerTextObj   `bson:"promocusttextdocid" json:"promocusttextdocid"` // promoCustomerTextObj document id
	SMSDocID           bson.ObjectId          `bson:"smsdocid" json:"smsdocid"`                     // SMS Message document id
	PsendDocID         bson.ObjectId          `bson:"psenddocid" json:"psenddocid"`                 // promotion broadcast (send) document id
	PromoDocID         bson.ObjectId          `bson:"promodocid" json:"promodocid"`                 // promotion document id
	CustDocID          bson.ObjectId          `bson:"custdocid" json:"custdocid"`                   // end customer document id
	PromoType          string                 `bson:"promotype" json:"promotype"`                   // promotion type: standard, general
	PhoneNumber        string                 `bson:"phonenumber" json:"phonenumber"`               // end customer phone number
	ShortLink          string                 `bson:"shortlink" json:"shortlink"`                   // shortlink associated with the text
	ShortCode          string                 `bson:"shortcode" json:"shortcode"`                   // shortcode associated with the text
	EventType          string                 `bson:"eventtype" json:"eventtype"`                   // type of event
	EventTime          time.Time              `bson:"eventtime" json:"eventtime"`                   // type of event
	EventContent       map[string]interface{} `bson:"eventcontent" json:"eventcontent"`             // event content
}
