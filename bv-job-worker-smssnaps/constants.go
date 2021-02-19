package main

const (
	cntAppEnvDev   = "development"
	cntAppEnvStage = "staging"
	cntAppEnvProd  = "production"

	cntPromoTypeStandard = "standard" // a 'standard' promotion
	cntPromoTypeGeneral  = "general"  // a 'general' or generic promotion

	cntDefaultObjectID string = "886e09000000000000000000" // Default bson.ObjectId string value

	// MongoDB database collections
	cntMgoCollSMS       = "smsmessages"
	cntMgoCollPSend     = "promobroadcasts"
	cntMgoCollPromo     = "promotions"
	cntMgoCollPromoGen  = "promotionsgeneral"
	cntMgoCollProdCards = "productcards"
	cntMgoCollEvents    = "logevents"

	cntKeenIOCollectionName = "smsmessages"     // Keen.io collection
	cntEventSendSMSMessage  = "smsmessage_sent" // sms message 'sent' event
)
