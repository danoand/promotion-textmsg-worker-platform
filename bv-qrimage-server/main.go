package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"

	"github.com/danoand/utils"
	"github.com/gin-gonic/gin"
	"github.com/globalsign/mgo"
)

var defBytes []byte
var defMap = make(map[string]string)

func main() {
	// Grab the port from an environment variable
	port := fmt.Sprintf(":%v", os.Getenv("PORT"))
	dbURL := os.Getenv("DB_MONGODB_DIGITAL_OCEAN") // reference the MongoDB migrated to Digital Ocean (temp solution)
	if len(dbURL) == 0 {
		// missing DB_MONGODB_DIGITAL_OCEAN environment variable
		log.Printf("FATAL: missing DB_MONGODB_DIGITAL_OCEAN environment variable\n")
		os.Exit(1)
	}

	log.Printf("DEBUG: dbURL is: %v\n", dbURL)

	mgoSession, err := mgo.Dial(dbURL)
	if err != nil {
		log.Printf("FATAL: %v - fatal error dialing the hosted Mongodb database. See: %v\n", utils.FileLine(), err)
		os.Exit(1)
	}
	mgoDB := mgoSession.DB("qrcode-img-db")
	mgoGridFiles := mgoDB.GridFS("fs")

	// Read the default image file into memory
	defaultFile, err := mgoGridFiles.Open("no_qr_image_found.png")
	if err != nil {
		log.Printf("ERROR: %v - error fetching the default image from the MongoDB database. See: %v\n",
			utils.FileLine(),
			err)
	}
	defer defaultFile.Close()

	defBytes, err = ioutil.ReadAll(defaultFile)
	if err != nil {
		log.Printf("ERROR: %v - error reading the default image file into memory. See: %v\n",
			utils.FileLine(),
			err)
	}

	// Configure the gin web server
	r := gin.Default()
	r.GET("/:fname", func(c *gin.Context) {
		fname := c.Param("fname")

		// missing filename?
		if len(fname) == 0 {
			// no filename - return a default image
			log.Printf("INFO: %v - serving the default image of %v bytes to the client\n",
				utils.FileLine(),
				len(defBytes))
			c.Data(http.StatusOK, "image/png", defBytes)
			return
		}

		// Fetch the file from the database
		image, err := mgoGridFiles.Open(fname)
		if err != nil {
			// error fetching the file from the database
			log.Printf("INFO: %v - serving the default image of %v bytes to the client\n",
				utils.FileLine(),
				len(defBytes))
			c.Data(http.StatusOK, "image/png", defBytes)
			return
		}
		defer image.Close()

		// Get stats on the image file
		c.DataFromReader(http.StatusOK, image.Size(), "image/png", image, defMap)

		return
	})

	log.Printf("INFO: %v - starting the image server on port: %v\n",
		utils.FileLine(),
		port)
	r.Run(port)
}
