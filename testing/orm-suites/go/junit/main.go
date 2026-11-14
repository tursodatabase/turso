// Command junit converts `go test -json` output on stdin into JUnit XML on
// stdout. Unlike gotestsum/go-junit-report, it puts the first real error line
// of each failing test into the failure message, so score.py can group
// failures by cause instead of lumping them all under "Failed".
package main

import (
	"bufio"
	"encoding/json"
	"encoding/xml"
	"os"
	"regexp"
	"strings"
)

type event struct {
	Action  string
	Package string
	Test    string
	Output  string
	Elapsed float64
}

type failure struct {
	Message string `xml:"message,attr"`
	Text    string `xml:",chardata"`
}

type testcase struct {
	Classname string    `xml:"classname,attr"`
	Name      string    `xml:"name,attr"`
	Time      float64   `xml:"time,attr"`
	Failure   *failure  `xml:"failure,omitempty"`
	Skipped   *struct{} `xml:"skipped,omitempty"`
}

type testsuites struct {
	XMLName xml.Name   `xml:"testsuites"`
	Cases   []testcase `xml:"testsuite>testcase"`
}

var (
	fileLine  = regexp.MustCompile(`^\s*[^\s:]+\.go:\d+: ?(.*)$`)
	testifyEr = regexp.MustCompile(`^\s*Error:\s+(.*)$`)
)

func main() {
	output := map[string]*strings.Builder{}
	var cases []testcase
	scanner := bufio.NewScanner(os.Stdin)
	scanner.Buffer(make([]byte, 1<<20), 64<<20)
	for scanner.Scan() {
		var e event
		if json.Unmarshal(scanner.Bytes(), &e) != nil || e.Test == "" {
			continue
		}
		key := e.Package + "\x00" + e.Test
		if output[key] == nil {
			output[key] = &strings.Builder{}
		}
		switch e.Action {
		case "output":
			output[key].WriteString(e.Output)
		case "pass", "fail", "skip":
			c := testcase{Classname: e.Package, Name: e.Test, Time: e.Elapsed}
			if e.Action == "skip" {
				c.Skipped = &struct{}{}
			} else if e.Action == "fail" {
				text := output[key].String()
				c.Failure = &failure{Message: firstError(text), Text: text}
			}
			cases = append(cases, c)
		}
	}
	os.Stdout.WriteString(xml.Header)
	enc := xml.NewEncoder(os.Stdout)
	enc.Indent("", " ")
	if err := enc.Encode(testsuites{Cases: cases}); err != nil {
		panic(err)
	}
}

// firstError returns the first line that looks like an error report: a
// testify "Error:" line, a t.Errorf "file.go:N: msg" line, or a panic.
func firstError(text string) string {
	lines := strings.Split(text, "\n")
	for i, line := range lines {
		if m := fileLine.FindStringSubmatch(line); m != nil && strings.TrimSpace(m[1]) != "" {
			msg := m[1]
			// Assertion helpers often print their caller's path again.
			for m = fileLine.FindStringSubmatch(msg); m != nil; m = fileLine.FindStringSubmatch(msg) {
				msg = m[1]
			}
			return strings.TrimSpace(msg)
		}
		if m := testifyEr.FindStringSubmatch(line); m != nil {
			msg := strings.TrimSpace(m[1])
			// testify puts the details of "Received unexpected error:" on the next line.
			if strings.HasSuffix(msg, ":") && i+1 < len(lines) {
				msg += " " + strings.TrimSpace(lines[i+1])
			}
			return msg
		}
		if strings.HasPrefix(strings.TrimSpace(line), "panic:") {
			return strings.TrimSpace(line)
		}
	}
	return "<no error line; parent test or timeout>"
}
