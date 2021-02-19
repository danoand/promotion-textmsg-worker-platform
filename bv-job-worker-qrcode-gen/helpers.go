package main

import (
	"log"

	"github.com/danoand/utils"
)

// Application environment types
const (
	cntAppEnvDev   = "development"
	cntAppEnvStage = "staging"
	cntAppEnvProd  = "production"
)

// Slice of environment types
var cntEnvironments = []string{
	cntAppEnvDev,
	cntAppEnvStage,
	cntAppEnvProd,
}

// hlprIsOneOfStr determines if a string is included in a slice of strings
func hlprIsOneOfStr(tst string, set []string) (rbool bool) {
	// Iterate through the set
	for i := 0; i < len(set); i++ {
		if tst == set[i] {
			rbool = true
			break
		}
	}

	return
}

// hlprRefreshMgoDB refreshes the MongoDB database session
func hlprRefreshMgoDB() {
	log.Printf("DEBUG: func hlprRefreshMgoDB - attempting to refresh the MongoDB sessions\n")
	mgoSession.Refresh()

	log.Printf("INFO: func hlprRefreshMgoDB - refreshed the MongoDB database sessions\n")

	return
}

// hlprHeartBeat writes to the log if the program is running on a cron job cadence
func hlprHeartBeat() {
	log.Printf("INFO: HEARTBEAT - %v - the %v program is up and running\n",
		utils.FileLine(),
		appName)

	return
}
