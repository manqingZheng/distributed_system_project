package main

import (
	"bytes"
	// "encoding/hex"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math"
	"net"
	"os"
	"strconv"
	"time"
    "math/rand"
	"gopkg.in/yaml.v2"
)

var eof_count int

type ServerConfigs struct {
	Servers []struct {
		ServerId int    `yaml:"serverId"`
		Host     string `yaml:"host"`
		Port     string `yaml:"port"`
	} `yaml:"servers"`
}

func readServerConfigs(configPath string) ServerConfigs {
	f, err := ioutil.ReadFile(configPath)

	if err != nil {
		log.Fatalf("could not read config file %s : %v", configPath, err)
	}

	scs := ServerConfigs{}
	err = yaml.Unmarshal(f, &scs)

	return scs
}

type InputLine struct {
	line []byte
	server_index int
}

func readInputFile(path string, n int) [] InputLine{
	file, err := os.Open(path)

	if err != nil {
		fmt.Println(err)
		os.Exit(-1)
	}

	defer file.Close()
	
    line_buf := make([]byte, 100)

	input_list := make([]InputLine, 0)
	for {
		_, err := io.ReadFull(file,line_buf)
		if err != nil {
			if err != io.EOF {
				fmt.Println(err)
				os.Exit(-1)
			}
			break
		}
		line_slice := make([]byte, len(line_buf))
		copy(line_slice, line_buf)
		
		server_index := get_n_bits(line_slice[0], n)

		input_list = append(input_list, InputLine{line:line_slice, server_index: server_index})
	}
	return input_list
}

func get_n_bits(a byte, n int) int {
	bit_string := ""
	for j := 7; j > 7-n; j-- {
		mask := byte(1 << uint(j))
		if (a & mask) == mask {
			bit_string = bit_string + "1"
		}else{
			bit_string = bit_string + "0"
		}
	}
	index_server, _ := strconv.ParseInt(bit_string, 2, 8)
	return int(index_server)
}

func handleConnection(conn net.Conn, received chan<- []byte) {
	for {
		buffer := make([]byte, 100)
		_, err := io.ReadFull(conn,buffer)
		if err != nil{
			if err != io.EOF{
				log.Panicln(err)
			}
			// log.Println("this is eof")
			eof_count += 1
			break
		}
		// temp := get_n_bits(buffer[0], 2)
		// log.Println("received string", len_read, hex.EncodeToString(buffer[:10]), "bits", temp) 
		received <- buffer
	}
}

func ReceiveConnection(listener net.Listener, received chan<- []byte) {
	// receive data from all other servers
	for{
		conn, err := listener.Accept()
		if err != nil{
			log.Panicln(err)
		}
		go handleConnection(conn, received)
	}
}

func SendOneLine(conn_list []net.Conn, input_list []InputLine, serverId int) {
	for _, input_line := range input_list {
		if (input_line.server_index == serverId) {
			// log.Println("keeping line", ind)
			continue
		}
		// log.Println("sending line", ind, "to", input_line.server_index, hex.EncodeToString(input_line.line[:10]))
		_, err := conn_list[input_line.server_index].Write(input_line.line)
		if (err != nil) {
			log.Println(err)
		}
	}
	for _, conn := range conn_list {
		if conn != nil {
			// log.Println("shut down conn", con_ind)
			conn.Close()
		}
	}
}

func ConsolidateReceived(byte_slice_list *[][]byte, received <- chan []byte, stop_sign <- chan bool) {
	for {
		select {
		case <- stop_sign:
			// log.Println("break for loop")
			return
		case received_slice := <- received:
			received_slice_copy := make([]byte, len(received_slice))
			copy(received_slice_copy, received_slice)
			*byte_slice_list = append(*byte_slice_list, received_slice_copy)
			// log.Println("consolidate copy", hex.EncodeToString(received_slice_copy[:10]))
		}
	}
}

func quicksort(a [][]byte) [][]byte {
	if len(a) < 2 {
		return a
	}

	left, right := 0, len(a) - 1

	pivot := rand.Int() % len(a)

	a[pivot], a[right] = a[right], a[pivot]

	for i := range a {
		if bytes.Compare(a[i][:10], a[right][:10]) < 0 {
			a[left], a[i] = a[i], a[left]
			left++
		}
	}

	a[left], a[right] = a[right], a[left]

	quicksort(a[:left])
	quicksort(a[left+1:])

	return a
}

func WriteOutput(s [][]byte, output_name string) {
	f, err := os.Create(output_name)
	if err != nil {
		log.Panicln(err)
	}
	defer f.Close()

	for _, line := range s {
		_, err := f.Write(line)
		if err != nil {
			log.Panicln(err)
		}
	}
}

func main() {
	eof_count = 0
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	if len(os.Args) != 5 {
		log.Fatal("Usage : ./netsort {serverId} {inputFilePath} {outputFilePath} {configFilePath}")
	}

	// What is my serverId
	serverId, err := strconv.Atoi(os.Args[1])
	if err != nil {
		log.Fatalf("Invalid serverId, must be an int %v", err)
	}
	// fmt.Println("My server Id:", serverId)

	// Read server configs from file
	scs := readServerConfigs(os.Args[4])
	// fmt.Println("Got the following server configs:", scs)

	num_severs := len(scs.Servers)
	n := int(math.Log2(float64(num_severs)))
	input_list := readInputFile(os.Args[2], n)

	// create receiver
	// log.Println("start to create receiver")
	listener, err := net.Listen("tcp", scs.Servers[serverId].Host + ":" + scs.Servers[serverId].Port)
	if err != nil {
		log.Panicln(err)
	}
	defer listener.Close()

	// log.Println("start to receive data")
	my_ch := make(chan []byte)
	go ReceiveConnection(listener, my_ch)
	time.Sleep(200 * time.Millisecond)

	// create senders 
	// log.Println("start to create senders")
	conn_list := make([]net.Conn, 0)
	for i := 0; i < num_severs; i++ {
		if (i == serverId) {
			conn_list = append(conn_list, nil)
			continue
		}
		conn, err := net.Dial("tcp", scs.Servers[i].Host + ":" + scs.Servers[i].Port)
		if err != nil{
			log.Panicln(err)
		}
		defer conn.Close()
		conn_list = append(conn_list, conn)
	}

	// send data to each server according to their first N bits 
	// log.Println("start to send data, total lines", len(input_list))
	go SendOneLine(conn_list, input_list, serverId)

	// sort
	// log.Println("start to consolidate")
	stop_sign := make(chan bool)
	var byte_slice_list [][]byte
	go ConsolidateReceived(&byte_slice_list, my_ch, stop_sign)

	for {
		if (eof_count == num_severs - 1) {
			// log.Println("complete")
			stop_sign <- true
			break
		}
	}

	// log.Println("after receive, number of lines", len(byte_slice_list))

	// for ind, recei := range byte_slice_list {
	// 	log.Println("after received ", ind, hex.EncodeToString(recei[:10])) 
	// }

	// get this server input
	for _, input_line := range input_list {
		if (input_line.server_index != serverId) {
			continue
		}
		// log.Println("add this server input", ind)
		input_line_copy := make([]byte, len(input_line.line))
		copy(input_line_copy, input_line.line)
		byte_slice_list = append(byte_slice_list, input_line_copy)
	}

	// log.Println("before sort, number of lines", len(byte_slice_list))
	sorted_byte_slice_list := quicksort(byte_slice_list)
	
	// log.Println("sorted, number of lines", len(sorted_byte_slice_list))
	// for _, line := range sorted_byte_slice_list {
	// 	log.Println("sorted string", hex.EncodeToString(line[:10])) 
	// }

	WriteOutput(sorted_byte_slice_list, os.Args[3])

	os.Exit(0)
}
