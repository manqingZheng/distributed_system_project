package tritonhttp

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

type Server struct {
	// Addr specifies the TCP address for the server to listen on,
	// in the form "host:port". It shall be passed to net.Listen()
	// during ListenAndServe().
	Addr string // e.g. ":0"

	// DocRoot specifies the path to the directory to serve static files from.
	DocRoot string
}

// ListenAndServe listens on the TCP network address s.Addr and then
// handles requests on incoming connections.
func (s *Server) ListenAndServe() error {
	// Validate the configuration of the server
	if err := s.ValidateServerSetup(); err != nil {
		return fmt.Errorf("server is not setup correctly %v", err)
	}
	fmt.Println("Server setup valid!")

	// server should now start to listen on the configured address
	ln, err := net.Listen("tcp", s.Addr)
	if err != nil {
		return err
	}
	fmt.Println("Listening on", ln.Addr())

	// making sure the listener is closed when we exit
	defer func() {
		err = ln.Close()
		if err != nil {
			fmt.Println("error in closing listener", err)
		}
	}()

	// accept connections forever
	for {
		conn, err := ln.Accept()
		if err != nil {
			continue
		}
		fmt.Println("accepted connection", conn.RemoteAddr())
		go s.HandleConnection(conn)
	}
}

// HandleConnection reads requests from the accepted conn and handles them.
func (s *Server) HandleConnection(conn net.Conn) {
	br := bufio.NewReader(conn)
	for {
		// Hint: use the other methods below
		// Set timeout
		if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
			log.Printf("Failed to set timeout for connection %v", conn)
			_ = conn.Close()
			return
		}

		req, IsbytesReceived, err := ReadRequest(br)

		if err == nil {
			log.Println("received good request")

			res := s.HandleGoodRequest(req)
			res.Write(conn)
			if res.Request.Close {
				conn.Close()
				return
			}

		} else if errors.Is(err, io.EOF) {
			log.Printf("Connection closed by %v", conn.RemoteAddr())
			_ = conn.Close()
			return
		} else if temp_err, ok := err.(net.Error); ok && temp_err.Timeout() {
			log.Printf("Connection to %v timed out", conn.RemoteAddr())
			if IsbytesReceived {
				log.Printf("But bytes received")
				res := &Response{}
				res.HandleBadRequest()
				res.Header["Connection"] = "close"
				res.Write(conn)
			}
			conn.Close()
			return
		} else if err.Error() == "400" {
			log.Println("received 400 request")
			res := &Response{}
			res.HandleBadRequest()
			res.Header["Connection"] = "close"
			res.Write(conn)
			conn.Close()
			return
		} else {
			log.Println(err.Error())
			return
		}
	}

}

// HandleGoodRequest handles the valid req and generates the corresponding res.
func (s *Server) HandleGoodRequest(req *Request) (res *Response) {
	// determine if escaped root
	url_error := false
	parsed_url := ""
	if strings.HasSuffix(req.URL, "/") {
		parsed_url = req.URL + "index.html"
	} else {
		parsed_url = req.URL
	}
	abs_path := filepath.Join(s.DocRoot, parsed_url)
	if strings.HasPrefix(parsed_url, "../") {
		url_error = true
	} else {
		if _, err := os.Stat(abs_path); errors.Is(err, os.ErrNotExist) {
			url_error = true
		}
	}

	res = &Response{}
	if !url_error {
		res.HandleOK(req, abs_path)
	} else {
		res.HandleNotFound(req)
	}

	return res

	// Hint: use the other methods below
}

// HandleOK prepares res to be a 200 OK response
// ready to be written back to client.
func (res *Response) HandleOK(req *Request, path string) {
	res.StatusCode = statusOK
	res.Proto = responseProto
	res.Request = req
	res.FilePath = path

	response_header := make(map[string]string)
	base_path := filepath.Ext(path)
	content_type := MIMETypeByExtension(base_path)
	file, err := os.Stat(path)
	if err != nil {
		log.Println("os Stat file error", err)
	}
	last_modified := FormatTime(file.ModTime())
	content_length := file.Size()
	response_header["Date"] = FormatTime(time.Now())
	response_header["Last-Modified"] = last_modified
	response_header["Content-Type"] = content_type
	response_header["Content-Length"] = strconv.FormatInt(content_length, 10)
	if res.Request.Close {
		response_header["Connection"] = "close"
	}
	for k, v := range req.Header {
		response_header[k] = v
	}

	res.Header = response_header
}

// HandleBadRequest prepares res to be a 400 Bad Request response
// ready to be written back to client.
func (res *Response) HandleBadRequest() {
	res.StatusCode = statusBadRequest
	res.Proto = responseProto
	res.FilePath = ""
	response_header := make(map[string]string)
	response_header["Date"] = FormatTime(time.Now())
	res.Header = response_header
}

// HandleNotFound prepares res to be a 404 Not Found response
// ready to be written back to client.
func (res *Response) HandleNotFound(req *Request) {
	res.StatusCode = statusNotFound
	res.Proto = responseProto
	res.Request = req
	res.FilePath = ""
	response_header := make(map[string]string)
	response_header["Date"] = FormatTime(time.Now())
	if res.Request.Close {
		response_header["Connection"] = "close"
	}
	res.Header = response_header
}

func (s *Server) ValidateServerSetup() error {
	// Validating the doc root of the server
	fi, err := os.Stat(s.DocRoot)

	if os.IsNotExist(err) {
		return err
	}

	if !fi.IsDir() {
		return fmt.Errorf("doc root %q is not a directory", s.DocRoot)
	}

	return nil
}
