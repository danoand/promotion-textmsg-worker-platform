package wrkrcom

import (
	"errors"
	"log"
	"net/http"
	"time"

	faktory "github.com/contribsys/faktory/client"
	worker "github.com/contribsys/faktory_worker_go"
	"github.com/gin-gonic/gin"
	"github.com/globalsign/mgo"
)

var (
	// CntStopAllWorkers directive to halt worker jobs from continuing
	CntStopAllWorkers = "STOP_WORKERS"
	// CntWorkerNameSendSMSMessages sms msg job name
	CntWorkerNameSendSMSMessages = "bv-job-worker-smsmsgs"
	// CntWorkerNameQRCodeGen qrcode generation job name
	CntWorkerNameQRCodeGen = "bv-job-worker-qrcode-gen"
	// CntWorkerNameSMSSnaps sms snapshot job name
	CntWorkerNameSMSSnaps = "bv-job-worker-smssnaps"
	// ErrClientNotSet indicates that a Faktory client has not been set
	ErrClientNotSet = errors.New("Faktory client not set")
)

// Domain houses data provided to a handler
type Domain struct {
	Mgo          *mgo.Session
	MgoDB        *mgo.Database
	FakClientOK  bool
	FakClient    *faktory.Client
	FakClientErr error
}

// NewDomain constructs a domain object
func NewDomain() (Domain, error) {
	var rtn Domain

	rtn.FakClient, rtn.FakClientErr = faktory.Open()
	if rtn.FakClientErr == nil {
		// successfully created a Faktory client
		rtn.FakClientOK = true
	}
	return rtn, rtn.FakClientErr
}

// HndlrStatus returns the status of the Faktory client
func (dmn Domain) HndlrStatus(c *gin.Context) {
	var err error
	var tMap = make(map[string]interface{})
	var rMap = make(map[string]interface{})

	// Has a Faktory client object been generated and
	if !dmn.FakClientOK {
		// Faktory client error has occurred
		rMap["msg"] = "Faktory client error has occurred"
		rMap["err"] = dmn.FakClientErr
		c.JSON(http.StatusInternalServerError, rMap)
		return
	}

	tMap, err = dmn.FakClient.Info()
	if err != nil {
		rMap["msg"] = "error occurred querying the Faktory client"
		rMap["err"] = err.Error()
		c.JSON(http.StatusInternalServerError, rMap)
		return
	}

	rMap["msg"] = "response from the Faktory client"
	rMap["content"] = tMap
	c.JSON(http.StatusOK, rMap)
}

// GenerateHandlrStatus return a gin handler
func (dmn Domain) GenerateHandlrStatus() gin.HandlerFunc {
	return func(c *gin.Context) {
		var err error
		var tMap = make(map[string]interface{})
		var rMap = make(map[string]interface{})

		// Has a Faktory client object been generated and
		if !dmn.FakClientOK {
			// Faktory client error has occurred
			rMap["msg"] = "Faktory client error has occurred"
			rMap["err"] = dmn.FakClientErr
			c.JSON(http.StatusInternalServerError, rMap)
			return
		}

		tMap, err = dmn.FakClient.Info()
		if err != nil {
			rMap["msg"] = "error occurred querying the Faktory client"
			rMap["err"] = err.Error()
			c.JSON(http.StatusInternalServerError, rMap)
			return
		}

		rMap["msg"] = "response from the Faktory client"
		rMap["content"] = tMap
		c.JSON(http.StatusOK, rMap)
	}
}

// DummyFunc executes a dummy worker function
func (dmn Domain) DummyFunc(ctx worker.Context, args ...interface{}) error {
	log.Println("INFO: executing DummyFunc at time:", time.Now().Format(time.RFC3339))
	return nil
}
