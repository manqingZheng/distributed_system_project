package tritonhttp

import (
	"bufio"
	"fmt"
	"io"

	// "time"
	"os"
	"sort"
)

const (
	responseProto = "HTTP/1.1"

	statusOK         = 200
	statusBadRequest = 400
	statusNotFound   = 404
)

var statusText = map[int]string{
	statusOK:         "OK",
	statusBadRequest: "Bad Request",
	statusNotFound:   "Not Found",
}

type Response struct {
	StatusCode int    // e.g. 200
	Proto      string // e.g. "HTTP/1.1"

	// Header stores all headers to write to the response.
	// Header keys are case-incensitive, and should be stored
	// in the canonical format in this map.
	Header map[string]string

	// Request is the valid request that leads to this response.
	// It could be nil for responses not resulting from a valid request.
	Request *Request

	// FilePath is the local path to the file to serve.
	// It could be "", which means there is no file to serve.
	FilePath string
}

// Write writes the res to the w.
func (res *Response) Write(w io.Writer) error {
	if err := res.WriteStatusLine(w); err != nil {
		return err
	}
	if err := res.WriteSortedHeaders(w); err != nil {
		return err
	}
	if err := res.WriteBody(w); err != nil {
		return err
	}
	return nil
}

// WriteStatusLine writes the status line of res to w, including the ending "\r\n".
// For example, it could write "HTTP/1.1 200 OK\r\n".
func (res *Response) WriteStatusLine(w io.Writer) error {
	bw := bufio.NewWriter(w)
	status_line := fmt.Sprintf("%v %v %v\r\n", res.Proto, res.StatusCode, statusText[res.StatusCode])
	_, err := bw.WriteString(status_line)
	if err != nil {
		return err
	}
	bw.Flush()
	return nil
}

// WriteSortedHeaders writes the headers of res to w, including the ending "\r\n".
// For example, it could write "Connection: close\r\nDate: foobar\r\n\r\n".
// For HTTP, there is no need to write headers in any particular order.
// TritonHTTP requires to write in sorted order for the ease of testing.
func (res *Response) WriteSortedHeaders(w io.Writer) error {
	bw := bufio.NewWriter(w)

	key_list := make([]string, 0)
	for k := range res.Header {
		key_list = append(key_list, k)
	}
	sort.Strings(key_list)

	for _, k := range key_list {
		line := fmt.Sprintf("%v: %v\r\n", k, res.Header[k])
		_, err := bw.WriteString(line)
		if err != nil {
			return err
		}
	}

	_, err := bw.WriteString("\r\n")
	if err != nil {
		return err
	}

	bw.Flush()
	return nil
}

// WriteBody writes res' file content as the response body to w.
// It doesn't write anything if there is no file to serve.
func (res *Response) WriteBody(w io.Writer) error {
	if res.FilePath == "" {
		return nil
	}
	bw := bufio.NewWriter(w)
	f, err := os.Open(res.FilePath)
	if err != nil {
		return err
	}
	buffer := make([]byte, 1000000)
	for {
		n, err := f.Read(buffer)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		_, err = bw.Write(buffer[:n])
		if err != nil {
			return err
		}
		bw.Flush()
	}
	return nil
}
