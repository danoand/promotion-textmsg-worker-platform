// cronfuncs.go contains code to define, schedule, and manage scheduled and recurring "jobs" (functions)
package main

import (
	_ "log"

	"github.com/danoand/utils"
	"github.com/jasonlvhit/gocron"
)

// schedJobs defines the cron jobs (called when starting the application up)
func schedJobs() {
	appLog("INFO: define cron jobs\n")

	// Define recurring jobs
	gocron.Every(4).Hours().Do(func() {
		// Refresh the production MongoDB database connection
		mgoSession.Refresh()
		appLog("INFO: %v - refreshed the production MongoDB database session\n", utils.FileLine())
		return
	})
	gocron.Every(30).Minutes().Do(func() {
		appLog("INFO: HEARTBEAT - %v - the %v program is up and running\n",
			utils.FileLine(),
			appName)
	})

	// Start the default gocron scheduler
	appLog("INFO: %v - start this web service's 'cron' scheduler\n", utils.FileLine())
	<-gocron.Start()

	appLog("INFO: %v - stop cron jobs\n", utils.FileLine())

	return
}
