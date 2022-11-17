package surfstore

import (
	context "context"
	"fmt"
	"sync"
	"google.golang.org/grpc"
	"log"
	"strings"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
	"time"

)

type RaftSurfstore struct {
	// TODO add any fields you need
	isLeader bool
	term     int64
	log      []*UpdateOperation
	metaStore *MetaStore

	// new
    commitIndex int64
	nextIndex []int64

	// Server Info
    ipList []string
    serverId int

	commit_permission_chan_list []chan bool

    // Leader protection
    // isLeaderMutex *sync.RWMutex
    // isLeaderCond *sync.Cond

    // rpcConns []*grpc.ClientConn

	/*--------------- Chaos Monkey --------------*/
	isCrashed      bool
	isCrashedMutex *sync.RWMutex
	notCrashedCond *sync.Cond

	UnimplementedRaftSurfstoreServer
}

func (s *RaftSurfstore) GetFileInfoMap(ctx context.Context, empty *emptypb.Empty) (*FileInfoMap, error) {
	log.Println("Log Server", s.serverId, "GetFileInfoMap")
	if (!s.isLeader) {
		log.Println("Log Server", s.serverId, "Not leader")
		return nil, fmt.Errorf("not leader")
	}
	return &FileInfoMap{FileInfoMap: s.metaStore.FileMetaMap}, nil
}

func (s *RaftSurfstore) GetBlockStoreAddr(ctx context.Context, empty *emptypb.Empty) (*BlockStoreAddr, error) {
	log.Println("Log Server", s.serverId, "GetBlockStoreAddr")
	
	return &BlockStoreAddr{Addr: s.metaStore.BlockStoreAddr}, nil
}

func (s *RaftSurfstore) UpdateFile(ctx context.Context, filemeta *FileMetaData) (*Version, error) {
	log.Println("Log Server", s.serverId, "UpdateFile")
	if (!s.isLeader) {
		log.Println("Log Server", s.serverId, "UpdateFile Not leader")
		return nil, fmt.Errorf("not leader")
	}

	op := UpdateOperation{
        Term: s.term,
        FileMetaData: filemeta,
    }

    s.log = append(s.log, &op)

	commit_permission_chan := make(chan bool)
	s.commit_permission_chan_list = append(s.commit_permission_chan_list, commit_permission_chan)

	go s.ReplicateLogToServers(ctx)

    success := <-commit_permission_chan
    if success {
		log.Println("Log Server", s.serverId, "UpdateFile success commit_permission_chan")
        return s.metaStore.UpdateFile(ctx, filemeta)
    }

    return nil, nil
}

func (s *RaftSurfstore) ReplicateLogToServers(ctx context.Context) {
	log.Println("Log Server", s.serverId, "ReplicateLogToServers")
	if len(s.log) == 0 {
		log.Fatal("Nothing in log")
	}
	current_target_log_idx := int(s.commitIndex) + 1
	receive_chan := make(chan *AppendEntryOutput, len(s.ipList) - 1)

	log.Println("Log Server", s.serverId, "ReplicateLogToServers for log index", current_target_log_idx)

	for i, _ := range s.ipList {
		if i == s.serverId {
			continue
		} else {
			go s.SendEntry(ctx, i, true, int(current_target_log_idx), receive_chan)
		}
	}

	reply_success_count := 1
	for {
		output := <- receive_chan
		log.Println("Log Server", s.serverId, "ReplicateLogToServers, received output from", output.ServerId, output.Success)
		if output.Success {
			reply_success_count += 1
		}
		if reply_success_count > len(s.ipList) / 2 {
			log.Println("Log Server", s.serverId, "ReplicateLogToServers, majority got")
			s.commit_permission_chan_list[current_target_log_idx-1] <- true
			s.commitIndex = int64(current_target_log_idx)
			break
		}
	}
}

//1. Reply false if term < currentTerm (§5.1)
//2. Reply false if log doesn’t contain an entry at prevLogIndex whose term
//matches prevLogTerm (§5.3)
//3. If an existing entry conflicts with a new one (same index but different
//terms), delete the existing entry and all that follow it (§5.3)
//4. Append any new entries not already in the log
//5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index
//of last new entry)
func (s *RaftSurfstore) AppendEntries(ctx context.Context, input *AppendEntryInput) (*AppendEntryOutput, error) {
	log.Println("Log Server", s.serverId, "AppendEntries")
	
	if s.isCrashed {
		log.Println("Log Server", s.serverId, "AppendEntries, server crashed")

		output := &AppendEntryOutput{
			ServerId: int64(s.serverId),
			Term: s.term,
			Success: false,
			MatchedIndex: -1,
		}
		return output, ERR_SERVER_CRASHED
	}
	if (input.Term < s.term) {
		log.Println("Log Server", s.serverId, "AppendEntries, input term < server term")

		output := &AppendEntryOutput{
			ServerId: int64(s.serverId),
			Term: s.term,
			Success: false,
			MatchedIndex: -1,
		}
		return output, ERR_TERM_MISMATCH
	}

	if (s.term < input.Term) {
		if s.isLeader {
			s.isLeader = false
			log.Println("Log Server", s.serverId, "AppendEntries, step down to follower")
		}
		s.term = input.Term
	}

	// assume always receive entries
	if len(input.Entries) > 0 {
		if (len(s.log) == 0 && input.PrevLogIndex == 0) || (input.PrevLogIndex <= int64(len(s.log)) && s.log[input.PrevLogIndex-1].Term == input.PrevLogTerm) {
			new_log := make([]*UpdateOperation, input.PrevLogIndex)
			copy(new_log, s.log[:input.PrevLogIndex])
			new_log = append(new_log, input.Entries...)
			s.log = new_log

			if input.LeaderCommit > s.commitIndex {
				if input.LeaderCommit < int64(len(s.log)) {
					s.commitIndex = input.LeaderCommit
				} else {
					s.commitIndex = int64(len(s.log))
				}
				s.metaStore.UpdateFile(ctx, s.log[s.commitIndex-1].FileMetaData)
				log.Println("Log Server", s.serverId, "AppendEntries, make commit index", s.commitIndex)
			}
			log.Println("Log Server", s.serverId, "AppendEntries, copied log")
		} else {
			output := &AppendEntryOutput{
				ServerId: int64(s.serverId),
				Term: s.term,
				Success: false,
				MatchedIndex: -1,
			}
			return output, nil
		}
	} else {
		if input.LeaderCommit > s.commitIndex {
			if input.LeaderCommit < int64(len(s.log)) {
				s.commitIndex = input.LeaderCommit
			} else {
				s.commitIndex = int64(len(s.log))
			}
			s.metaStore.UpdateFile(ctx, s.log[s.commitIndex-1].FileMetaData)
			log.Println("Log Server", s.serverId, "AppendEntries, make commit index", s.commitIndex)
		}
	}

	output := &AppendEntryOutput{
		ServerId: int64(s.serverId),
		Term: s.term,
		Success: true,
		MatchedIndex: -1,
	}
	return output, nil
}

// This should set the leader status and any related variables as if the node has just won an election
func (s *RaftSurfstore) SetLeader(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	log.Println("Log Server", s.serverId, "SetLeader")
	
	// s.isLeaderMutex.Lock()
	s.isLeader = true
	s.nextIndex = make([]int64, len(s.ipList))
	s.term += 1
	for i := range s.nextIndex {
		s.nextIndex[i] = int64(len(s.log))
	}
	// s.isLeaderMutex.Unlock()

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) SendEntry(ctx context.Context, server_idx int, new_entry_exists bool, target_log_idx int, receive_chan chan *AppendEntryOutput) {
	log.Println("Log Server", s.serverId, "SendEntry to", server_idx)
	
	conn, err := grpc.Dial(s.ipList[server_idx], grpc.WithInsecure())
	if err != nil {
		log.Fatal("err dialling")
	}
	c := NewRaftSurfstoreClient(conn)

	ctx2, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	for {
		var PrevLogTerm int64
		if len(s.log) == 0 {
			PrevLogTerm = -1
		} else {
			PrevLogTerm = s.log[s.nextIndex[server_idx]].Term
		}
		append_entry_input := &AppendEntryInput{
			Term: s.term,
			PrevLogIndex: s.nextIndex[server_idx],
			PrevLogTerm: PrevLogTerm,
			Entries: s.log[s.nextIndex[server_idx]:target_log_idx],
			LeaderCommit: s.commitIndex,
		}
		log.Println("Log Server", s.serverId, "SendEntry to", server_idx, 
					"Term:", append_entry_input.Term, "PrevLogIndex:", append_entry_input.PrevLogIndex,
					"PrevLogTerm", append_entry_input.PrevLogTerm, "Entries:", len(append_entry_input.Entries),
					"LeaderCommit:", append_entry_input.LeaderCommit)
		output, err := c.AppendEntries(ctx2, append_entry_input)
		if err != nil {
			if strings.Contains(err.Error(), "Term mismatch") {
				log.Println("Log Server", s.serverId, "SendEntry to", server_idx, err)
				break
			} else if strings.Contains(err.Error(), "Server is crashed"){
				log.Println("Log Server", s.serverId, "SendEntry to", server_idx, err)
				continue
			} else {
				log.Fatal(err)
				conn.Close()
			}
		} else {
			if output.Success {
				receive_chan <- output
				s.nextIndex[s.serverId] = int64(target_log_idx)
				log.Println("Log Server", s.serverId, "SendEntry", server_idx, "Set next index to", target_log_idx)
				log.Println("Log Server", s.serverId, "SendEntry, received success output from", server_idx)
				break
			} else {
				s.nextIndex[server_idx] -= 1
			}
		}
	}
	conn.Close()

}

func (s *RaftSurfstore) SendEmpty(ctx context.Context, server_idx int, receive_chan chan *AppendEntryOutput) {
	log.Println("Log Server", s.serverId, "SendEmpty to", server_idx)
	
	conn, err := grpc.Dial(s.ipList[server_idx], grpc.WithInsecure())
	if err != nil {
		log.Fatal("err dialling")
	}
	c := NewRaftSurfstoreClient(conn)

	ctx2, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	for {
		append_entry_input := &AppendEntryInput{
			Term: s.term,
			PrevLogIndex: -1,
			PrevLogTerm: -1,
			Entries: make([]*UpdateOperation, 0),
			LeaderCommit: s.commitIndex,
		}
		log.Println("Log Server", s.serverId, "SendEmpty to", server_idx, 
					"Term:", append_entry_input.Term, "PrevLogIndex:", append_entry_input.PrevLogIndex,
					"PrevLogTerm", append_entry_input.PrevLogTerm, "Entries:", len(append_entry_input.Entries),
					"LeaderCommit:", append_entry_input.LeaderCommit)
		output, err := c.AppendEntries(ctx2, append_entry_input)
		if err != nil {
			if strings.Contains(err.Error(), "Term mismatch") {
				log.Println(err)
				break
			} else if strings.Contains(err.Error(), "Server is crashed"){
				log.Println(err)
				continue
			} else {
				log.Fatal(err)
				conn.Close()
			}
		} else {
			receive_chan <- output
			log.Println("Log Server", s.serverId, "SendEmpty, received output from", server_idx, output.Success)
			break
		}
	}
	conn.Close()
}

// Send a 'Heartbeat" (AppendEntries with no log entries) to the other servers
// Only leaders send heartbeats, if the node is not the leader you can return Success = false
func (s *RaftSurfstore) SendHeartbeat(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	log.Println("Log Server", s.serverId, "SendHeartbeat")
	if (!s.isLeader) {
		return &Success{Flag: false}, nil
	}

	receive_chan := make(chan *AppendEntryOutput, len(s.ipList) - 1)
	for i, _ := range s.ipList {
		if i == s.serverId {
			continue
		} else {
			go s.SendEmpty(ctx, i, receive_chan)
		}
	}
	log.Println("Log Server", s.serverId, "SendHeartbeat")

	reply_success_count := 1
	for {
		output := <- receive_chan
		log.Println("Log Server", s.serverId, "SendHeartbeat, received output from", output.ServerId, output.Success)
		if output.Success {
			reply_success_count += 1
		}
		if reply_success_count > len(s.ipList) / 2 {
			log.Println("Log Server", s.serverId, "SendHeartbeat, majority got")
			break
		}
	}

	return  &Success{Flag: true}, nil
}

func (s *RaftSurfstore) Crash(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.isCrashedMutex.Lock()
	s.isCrashed = true
	s.isCrashedMutex.Unlock()

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) Restore(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.isCrashedMutex.Lock()
	s.isCrashed = false
	s.notCrashedCond.Broadcast()
	s.isCrashedMutex.Unlock()

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) IsCrashed(ctx context.Context, _ *emptypb.Empty) (*CrashedState, error) {
	return &CrashedState{IsCrashed: s.isCrashed}, nil
}

func (s *RaftSurfstore) GetInternalState(ctx context.Context, empty *emptypb.Empty) (*RaftInternalState, error) {
	fileInfoMap, _ := s.metaStore.GetFileInfoMap(ctx, empty)
	return &RaftInternalState{
		IsLeader: s.isLeader,
		Term:     s.term,
		Log:      s.log,
		MetaMap:  fileInfoMap,
	}, nil
}

var _ RaftSurfstoreInterface = new(RaftSurfstore)
