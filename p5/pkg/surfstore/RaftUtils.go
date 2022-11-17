package surfstore

import (
	"bufio"
	"google.golang.org/grpc"
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
)

func LoadRaftConfigFile(filename string) (ipList []string) {
	configFD, e := os.Open(filename)
	if e != nil {
		log.Fatal("Error Open config file:", e)
	}
	defer configFD.Close()

	configReader := bufio.NewReader(configFD)
	serverCount := 0

	for index := 0; ; index++ {
		lineContent, _, e := configReader.ReadLine()
		if e != nil && e != io.EOF {
			log.Fatal("Error During Reading Config", e)
		}

		if e == io.EOF {
			return
		}

		lineString := string(lineContent)
		splitRes := strings.Split(lineString, ": ")
		if index == 0 {
			serverCount, _ = strconv.Atoi(splitRes[1])
			ipList = make([]string, serverCount, serverCount)
		} else {
			ipList[index-1] = splitRes[1]
		}
	}

	return
}

func NewRaftServer(id int64, ips []string, blockStoreAddr string) (*RaftSurfstore, error) {
	// TODO any initialization you need to do here

	// init connection to every server
	// raft_servers := make([]RaftSurfstoreClient, len(ips))

	// for i, ip := range ips {
	// 	if int64(i) == id {
	// 		raft_servers = append(raft_servers, nil)
	// 	} else {
	// 		conn, err := grpc.Dial(ip, grpc.WithInsecure())
	// 		if err != nil {
	// 			log.Fatal("err dialling")
	// 		}
	// 		c := NewRaftSurfstoreClient(conn)
	// 		raft_servers = append(raft_servers, c)
	// 	}
	// }
	
	isCrashedMutex := &sync.RWMutex{}

	server := RaftSurfstore{
		// TODO initialize any fields you add here
		isLeader:       false,
		term:           0,
		log:            make([]*UpdateOperation, 0),
		metaStore:      NewMetaStore(blockStoreAddr),
		
		commitIndex: 0,

		ipList: ips,
		serverId: int(id),
		commit_permission_chan_list: make([]chan bool, 0),

		nextIndex: make([]int64, len(ips)),

		isCrashed:      false,
		notCrashedCond: sync.NewCond(isCrashedMutex),
		isCrashedMutex: isCrashedMutex,

	}

	return &server, nil
}

// TODO Start up the Raft server and any services here
func ServeRaftServer(server *RaftSurfstore) error {
	grpc_server := grpc.NewServer()
	RegisterRaftSurfstoreServer(grpc_server, server)
	listener, err := net.Listen("tcp", server.ipList[server.serverId])
    if err != nil {
        return err
    }
	grpc_server.Serve(listener)
	return nil
}
