// cronjobs.go contains code to define, schedule, and manage scheduled and recurring "jobs" (functions)
package main

import (
	"log"
	_ "log"

	"github.com/danoand/utils"
	"github.com/jasonlvhit/gocron"
)

// schedJobs defines the cron jobs (called when starting the application up)
func schedJobs() {
	log.Printf("INFO: %v - define cron jobs\n", utils.FileLine())

	gocron.Every(4).Hours().Do(hlprRefreshMgoDB) // Refresh the MongoDB database connection
	gocron.Every(1).Hours().Do(hlprHeartBeat)    // Log a heartbeat message

	// Fire any cron jobs that need to be run now
	go hlprRefreshMgoDB()

	// Start the default gocron scheduler
	log.Printf("INFO: %v - start 'cron' scheduler\n", utils.FileLine())
	<-gocron.Start()

	log.Printf("INFO: %v - stop cron jobs\n", utils.FileLine())

	return
}
