package tritonhttp

import (
	"bufio"
	"fmt"
	"path"
	"strings"
)

type Request struct {
	Method string // e.g. "GET"
	URL    string // e.g. "/path/to/a/file"
	Proto  string // e.g. "HTTP/1.1"

	// Header stores misc headers excluding "Host" and "Connection",
	// which are stored in special fields below.
	// Header keys are case-incensitive, and should be stored
	// in the canonical format in this map.
	Header map[string]string

	Host  string // determine from the "Host" header
	Close bool   // determine from the "Connection" header
}

func ParseRequestLine(line string) ([]string, error) {
	fields := strings.SplitN(line, " ", -1)
	if len(fields) != 3 {
		return []string{""}, fmt.Errorf("400")
	}
	method := fields[0]
	if method != "GET" {
		return []string{""}, fmt.Errorf("400")
	}
	version := fields[2]
	if version != "HTTP/1.1" {
		return []string{""}, fmt.Errorf("400")
	}
	url := fields[1]
	if len(url) < 1 || string([]rune(url)[0]) != "/" {
		return []string{""}, fmt.Errorf("400")
	}
	parsed_url := ""
	if strings.HasSuffix(url, "/") {
		parsed_url = url + "index.html"
	} else {
		parsed_url = url
	}

	parsed_url = path.Clean(parsed_url)



	return []string{method, parsed_url, version}, nil
}

func ParseHeaderLine(line string) (string, string, error) {
	fields := strings.SplitN(line, ":", 2)
	if len(fields) == 1 {
		return "", "", fmt.Errorf("400")
	}
	key := CanonicalHeaderKey(fields[0])
	value := strings.TrimLeft(fields[1], " ")
	if value == "" {
		return "", "", fmt.Errorf("400")
	}
	return key, value, nil
}

// ReadRequest tries to read the next valid request from br.
//
// If it succeeds, it returns the valid request read. In this case,
// bytesReceived should be true, and err should be nil.
//
// If an error occurs during the reading, it returns the error,
// and a nil request. In this case, bytesReceived indicates whether or not
// some bytes are received before the error occurs. This is useful to determine
// the timeout with partial request received condition.
func ReadRequest(br *bufio.Reader) (req *Request, bytesReceived bool, err error) {
	// Read start line
	line, err := ReadLine(br)
	if err != nil {
		// handle error
		return nil, false, err
	}
	// parse request line
	request_list, err := ParseRequestLine(line)
	if err != nil {
		return nil, true, err
	}
	// Read headers
	header := make(map[string]string)
	var host string
	has_host := false
	close:= false
	for {
		line, err := ReadLine(br)
		if line == "" {
			fmt.Println("end of file in request")
			break
		}
		if err != nil {
			// if err == io.EOF {
			// 	fmt.Println("end of file in request")
			// 	break
			// }
			fmt.Println(err)
		}
		key, value, err := ParseHeaderLine(line)
		if err != nil {
			return nil, true, err
		}

		if key == "Host" {
			host = value
			has_host = true
		} else if key == "Connection" && value == "close" {
			close = true
		} else {
			header[key] = value
		}
	}

	if !has_host {
		return nil, true, fmt.Errorf("400")
	}

	request := Request{
		Method: request_list[0],
		URL:    request_list[1],
		Proto:  request_list[2],
		Header: header,
		Host:   host,
		Close:  close,
	}

	return &request, true, nil
}
