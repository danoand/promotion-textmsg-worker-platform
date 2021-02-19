package main

import (
	"fmt"
	"regexp"
	"strings"
	"time"

	valid "github.com/asaskevich/govalidator"
)

var rgxNonDigit = regexp.MustCompile(`\D`)
var rgxPlusOnePrefix = regexp.MustCompile(`^\+1`)
var rgxPhoneTwilioFormat = regexp.MustCompile(`^\+1\d{10}$`)

// hlprIsPhone determines if a string contains 10 numeric digits
func hlprIsPhone(s string) (rbool bool) {
	tmpStr := valid.WhiteList(s, "0-9")

	if len(tmpStr) == 10 {
		rbool = true
	}

	return
}

// hlprTransPhone eliminates non-numeric characters from a string
func hlprTransPhone(s string) string {
	return rgxNonDigit.ReplaceAllString(s, "")
}

// hlprStripPlusOne strips "+1" from a string (phone number string)
func hlprStripPlusOne(s string) string {
	return rgxPlusOnePrefix.ReplaceAllString(s, "")
}

// hlprConfirmSlash ensures an ending, single forward slash is appended to a string
func hlprConfirmSlash(s string) string {
	// Is there a '/' at the end of the shortlink base url?
	if !strings.HasSuffix(s, "/") {
		// no '/' at the end of the specified shortlink base url
		s = s + "/"
	}

	return s
}

// hlprDispDateTime translates a time object to date/time in string format
func hlprDispDateTime(t time.Time) string {
	var tmpStr string

	// Get the time in the string format
	tmpStr = t.In(timePT).Format(time.RFC3339)

	return fmt.Sprintf("%v-%v-%v %v:%v", tmpStr[5:7], tmpStr[8:10], tmpStr[2:4], tmpStr[11:13], tmpStr[14:16])
}

// hlprIsOneOfStr determines if a constant is included in a slice of constants
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
