// models.go constain struct definitions used in this program
package main

import (
	"time"

	bson "github.com/globalsign/mgo/bson"
)

// constAppString is a string type
type constAppString string

// promotionSend models a intended or executed promotion broadcast
type promotionSend struct {
	ID                bson.ObjectId            `bson:"_id" json:"docid" structs:"docid"`               // document id
	Status            constAppString           `bson:"status" json:"status" structs:"status"`          // promotion send status
	IsDeleted         bool                     `bson:"isdeleted" json:"isdeleted" structs:"isdeleted"` // (Soft) delete indicator
	IsHalted          bool                     `bson:"ishalted" json:"ishalted" structs:"ishalted"`    // indicates if this broadcast was halted
	HaltedMsg         string                   `bson:"psendhaltedmsg" json:"psendhaltedmsg" structs:"psendhaltedmsg"`
	Description       string                   `bson:"psenddescription" json:"psenddescription" structs:"psenddescription"`                   // send description
	TagID             bson.ObjectId            `bson:"psendtagid" json:"psendtagid" structs:"psendtagid"`                                     // a "tag" object id that links documents created at the same time (a promo send created for multiple promos)
	PromoID           bson.ObjectId            `bson:"psendpromoid" json:"psendpromoid" structs:"psendpromoid"`                               // owning promotion document id
	PromoClass        string                   `bson:"psendpromoclass" json:"psendpromoclass" structs:"psendpromoclass"`                      // class of promotion: 'standard', 'general'
	OwnerID           bson.ObjectId            `bson:"psendownerid" json:"psendownerid" structs:"psendownerid"`                               // owning object document id
	OwnerType         constAppString           `bson:"psendownertype" json:"psendownertype" structs:"psendownertype"`                         // owning object type (e.g. dispensary)
	ProductID         bson.ObjectId            `bson:"psendproductid" json:"psendproductid" structs:"psendproductid"`                         // promotion product id
	CreateTime        time.Time                `bson:"psendcreatetime" json:"psendcreatetime" structs:"psendcreatetime"`                      // time of creation
	DisplayCreateTime string                   `bson:"psenddispcreatetime" json:"psenddispcreatetime" structs:"psenddispcreatetime"`          // use for display on the front end
	SendTime          time.Time                `bson:"psendsendtime" json:"psendsendtime" structs:"psendsendtime"`                            // time of send
	DisplaySendTime   string                   `bson:"psenddispsendtime" json:"psenddispsendtime" structs:"psenddispsendtime"`                // use for display on the front end
	Log               []string                 `bson:"psendlog" json:"psendlog" structs:"psendlog"`                                           // message log
	SendToAll         bool                     `bson:"psendsendtoall" json:"psendsendtoall" structs:"psendsendtoall"`                         // Send to all customers flag
	SendToSelCustOnly bool                     `bson:"psendsendtoselcustonly" json:"psendsendtoselcustonly" structs:"psendsendtoselcustonly"` // Send only to selected customers (overrides send to all)
	PostalCodes       []string                 `bson:"psendpostalcodes" json:"psendpostalcodes" structs:"psendpostalcodes"`                   // array of postal codes
	AreaCodes         []string                 `bson:"psendareacodes" json:"psendareacodes" structs:"psendareacodes"`                         // array of area codes
	Cities            []string                 `bson:"psendcities" json:"psendcities" structs:"psendcities"`                                  // array of cities
	CitiesSearch      []string                 `bson:"psendcitiessearch" json:"psendcitiessearch" structs:"psendcitiessearch"`                // array of city search strings
	PromoSnapShot     map[string]interface{}   `bson:"psendpromotion" json:"psendpromotion" structs:"psendpromotion"`                         // snapshot of the promotion at send time
	RecipientSnapShop []map[string]interface{} `bson:"psendrecipients" json:"psendrecipients" structs:"psendrecipients"`                      // snapshot of the promotion at send time
	PromoCardPath     string                   `bson:"psendpromocardpath" json:"psendpromocardpath" structs:"psendpromocardpath"`             // Path to display the promo card for this broadcast
	WorkerJobs        []string                 `bson:"psendworkerjobs" json:"psendworkerjobs" structs:"psendworkerjobs"`                      // Jobs queued to process text messages
	TestMode          bool                     `bson:"psendtestmode" json:"psendtestmode" structs:"psendtestmode"`                            // Indicate if the broadcast was sent in test mode
}
