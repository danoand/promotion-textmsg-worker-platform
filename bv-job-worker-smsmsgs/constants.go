package main

const (
	cntAppEnvDev   = "development"
	cntAppEnvStage = "staging"
	cntAppEnvProd  = "production"

	cntPromoTypeStandard = "standard" // a 'standard' promotion
	cntPromoTypeGeneral  = "general"  // a 'general' or generic promotion

	cntPromoSendNotSent = "not sent"
	cntPromoSendSent    = "sent"

	cntDefaultObjectID string = "886e09000000000000000000" // Default bson.ObjectId string value

	// MongoDB database collections
	cntMgoCollSMS      = "smsmessages"
	cntMgoCollPSend    = "promobroadcasts"
	cntMgoCollPromo    = "promotions"
	cntMgoCollPromoGen = "promotionsgeneral"
	cntMgoCollProd     = "products"
	cntMgoCollEvents   = "logevents"

	cntKeenIOCollectionName = "smsmessages"     // Keen.io collection
	cntEventSendSMSMessage  = "smsmessage_sent" // sms message 'sent' event
)

var cntPromoSendStatuses = []string{
	cntPromoSendNotSent,
	cntPromoSendSent,
}
