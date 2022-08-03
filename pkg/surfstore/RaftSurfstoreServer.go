package surfstore

import (
	context "context"
	"log"
	"math"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type RaftSurfstore struct {
	// TODO add any fields you need

	ip string
	ipList []string
	serverId int64
	commitIndex int64 // append to the operation log
	lastApplied int64 //apply to raftmetastore

	isLeader bool
	isLeaderMutex sync.RWMutex
	term     int64
	log      []*UpdateOperation
	pendingCommits []chan bool

	metaStore *MetaStore

	/*--------------- Chaos Monkey --------------*/
	isCrashed      bool
	isCrashedMutex sync.RWMutex
	notCrashedCond *sync.Cond

	UnimplementedRaftSurfstoreServer
}

func (s *RaftSurfstore) GetFileInfoMap(ctx context.Context, empty *emptypb.Empty) (*FileInfoMap, error) {

	s.isCrashedMutex.RLock()
	isCrashed := s.isCrashed
	s.isCrashedMutex.RUnlock()
	if(isCrashed) {
		return nil, ERR_SERVER_CRASHED
	}

	s.isLeaderMutex.RLock()
	isLeader := s.isLeader
	s.isLeaderMutex.RUnlock()
	if !isLeader {
		return nil, ERR_NOT_LEADER
	}

	for {
		majorityAlive, _ := s.SendHeartbeat(ctx, empty)
		if(majorityAlive.Flag) {
			break
		}
	}

	//if a majority of the nodes are working, should return the correct answer;
	//if a majority of the nodes are crashed, should block until a majority recover.
	return s.metaStore.GetFileInfoMap(ctx, empty)

}

func (s *RaftSurfstore) GetBlockStoreAddr(ctx context.Context, empty *emptypb.Empty) (*BlockStoreAddr, error) {
	s.isCrashedMutex.RLock()
	isCrashed := s.isCrashed
	s.isCrashedMutex.RUnlock()
	if isCrashed {
		return nil, ERR_SERVER_CRASHED
	}

	s.isLeaderMutex.RLock()
	isLeader := s.isLeader
	s.isLeaderMutex.RUnlock()
	if !isLeader {
		return nil, ERR_NOT_LEADER
	}

	for {
		majorityAlive, _ := s.SendHeartbeat(ctx, empty)
		if majorityAlive.Flag {
			break
		}
	}

	// fmt.Println("BlockAddr: ",s.metaStore.BlockStoreAddr)

	//if a majority of the nodes are working, should return the correct answer;
	//if a majority of the nodes are crashed, should block until a majority recover.

	return s.metaStore.GetBlockStoreAddr(ctx, empty)
}

func (s *RaftSurfstore) UpdateFile(ctx context.Context, filemeta *FileMetaData) (*Version, error) {
	op := UpdateOperation{
		Term:         s.term,
		FileMetaData: filemeta,
	}

	s.isCrashedMutex.RLock()
	isCrashed := s.isCrashed
	s.isCrashedMutex.RUnlock()

	if(isCrashed){
		return nil, ERR_SERVER_CRASHED
	}

	s.isLeaderMutex.RLock()
	isLeader := s.isLeader
	s.isLeaderMutex.RUnlock()
	if !isLeader {
		return nil, ERR_NOT_LEADER
	}

	var empty *emptypb.Empty
	for {
		majorityAlive, _ := s.SendHeartbeat(ctx, empty)
		if majorityAlive.Flag {
			break
		}
	}


	s.log = append(s.log, &op)
	println("commited to the leader's log")
	//copy this log to majority of servers and check whether succeeded
	output := s.attemptCommit()
	println("copy to follower's log success:",output)

	//if a majority of the nodes are working, should return the correct answer;
	//if a majority of the nodes are crashed, should block until a majority recover.
	if output {
		s.lastApplied = s.commitIndex
		println("Applied to leader's meta store")
		v,e :=s.metaStore.UpdateFile(ctx, filemeta)
		println("123123123123")
		PrintMetaMap(s.metaStore.FileMetaMap)
		return v,e
	}

	return nil, ERR_SERVER_CRASHED
}

/*
 * send appendEntry request to all the rest servers concurrently
 * when succeeded, leader server commit first and then followers commit
 * 
*/
func (s *RaftSurfstore) attemptCommit()(bool) {
	targetIdx := s.commitIndex + 1
	output := make([]AppendEntryOutput,len(s.ipList))
	for idx, _ := range s.ipList {
		println(idx,s.serverId)
		if int64(idx) == s.serverId {
			continue
		}
		output[idx] = AppendEntryOutput{Success: false,Term: s.term,MatchedIndex: 0,ServerId: int64(idx)}
		go s.commitEntry(int64(idx), targetIdx,&output[idx])
		// s.commitEntry(int64(idx), targetIdx,&output[idx])
	}

	for {
		s.isCrashedMutex.RLock()
		isCrashed := s.isCrashed
		s.isCrashedMutex.RUnlock()
		if isCrashed {
			return false
		}

		commitCount := 1
		for idx, _ := range s.ipList {
			if(idx == int(s.serverId)){
				continue
			}else{
				if(output[idx].Success && output[idx].MatchedIndex == targetIdx) {
					commitCount++
				}
			}
		}

		if commitCount >= (len(s.ipList)+1)/2 {
			s.commitIndex = targetIdx
			break
		}
		// println("12312312323")
	}
	return true
}

func (s *RaftSurfstore) commitEntry(serverIdx, entryIdx int64, output *AppendEntryOutput)(error) {
	s.isCrashedMutex.RLock()
	isCrashed := s.isCrashed
	s.isCrashedMutex.RUnlock()
	// println(isCrashed , "serverID: ",serverIdx)
	
	if isCrashed {
		return nil
	}

	addr := s.ipList[serverIdx]
	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	client := NewRaftSurfstoreClient(conn)

	// TODO create correct AppendEntryInput from s.nextIndex, etc
	var prevLogTerm int64
	if entryIdx == 0 {
		prevLogTerm = 0
	} else {
		prevLogTerm = s.log[entryIdx-1].Term
	}

	input := &AppendEntryInput{
		Term:         s.term,
		PrevLogIndex: entryIdx - 1,
		PrevLogTerm:  prevLogTerm,
		Entries:      s.log[:entryIdx+1],
		LeaderCommit: entryIdx, ///////
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	var  curOutput *AppendEntryOutput
	curOutput, err = client.AppendEntries(ctx, input)
	if(err != nil){
		if strings.Contains(err.Error(), ERR_SERVER_CRASHED.Error()) {
			return err
		}else{
			log.Fatal("AppendEntry Error",err)
		}
	}

	println(serverIdx)
	println("commit to followers : ",serverIdx, curOutput.Success)

	for(true){
		if(curOutput.Success && curOutput.MatchedIndex == entryIdx){
			break
		}else if(curOutput.Success){
			input.PrevLogIndex++
			input.PrevLogTerm = s.log[input.PrevLogIndex].Term
			curOutput, _ = client.AppendEntries(ctx, input)
		}else{
			input.PrevLogIndex--
			input.PrevLogTerm = s.log[input.PrevLogIndex].Term
			curOutput, _ = client.AppendEntries(ctx, input)
		}
	}
	*output = *curOutput
	return nil
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
	output := &AppendEntryOutput{
		Success:      false,
		MatchedIndex: -1,
	}

	s.isCrashedMutex.RLock()
	isCrashed := s.isCrashed
	s.isCrashedMutex.RUnlock()
	if isCrashed {
		return output, ERR_SERVER_CRASHED
	}

	if input.Term > s.term {
		s.term = input.Term
		s.isLeaderMutex.Lock()
		s.isLeader = false
		s.isLeaderMutex.Unlock()
	}

	//1. Reply false if term < currentTerm (§5.1)
	if(input.Term<s.term){
		return output,nil
	}

	//2. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
	if(input.PrevLogIndex>=0 && s.log[input.PrevLogIndex].Term != input.PrevLogTerm){
		return output,nil
	}

	//3. If an existing entry conflicts with a new one (same index but different
	//terms), delete the existing entry and all that follow it (§5.3)
	//4. Append any new entries not already in the log
	if(int64(len(input.Entries))>input.PrevLogIndex+1){
		// println("AppendEnrty")
		// println(input.PrevLogIndex,len(s.log),len(input.Entries))
		if(len(s.log)<=int(input.PrevLogIndex+1)){
			s.log = append(s.log, input.Entries[input.PrevLogIndex+1])
		}else{
			s.log[input.PrevLogIndex+1] = input.Entries[input.PrevLogIndex+1]
		}
	}

	//5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index
	//of last new entry)
	// TODO only do this if leaderCommit > commitIndex

	if input.LeaderCommit > s.commitIndex {
		s.commitIndex = int64(math.Min(float64(input.LeaderCommit), float64(len(s.log)-1)))
		println("apply to metastore: ",s.ip,s.lastApplied, s.commitIndex)
		for s.lastApplied < s.commitIndex {
			s.lastApplied++
			entry := s.log[s.lastApplied]
			s.metaStore.UpdateFile(ctx, entry.FileMetaData)
		}
	}
	output.Success = true
	output.MatchedIndex = int64(input.PrevLogIndex+1)
	println(output.Success)
	return output, nil
}

// This should set the leader status and any related variables as if the node has just won an election
func (s *RaftSurfstore) SetLeader(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.isCrashedMutex.RLock()
	isCrashed := s.isCrashed
	s.isCrashedMutex.RUnlock()
	if isCrashed {
		return &Success{Flag: false}, ERR_SERVER_CRASHED
	}

	s.term++
	s.isLeaderMutex.Lock()
	s.isLeader = true
	s.isLeaderMutex.Unlock()
	return &Success{Flag: true}, nil
}

// Send a 'Heartbeat" (AppendEntries with no log entries) to the other servers
// Only leaders send heartbeats, if the node is not the leader you can return Success = false
/*
 * Used for checking whether there is a majority of servers alive
*/
func (s *RaftSurfstore) SendHeartbeat(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.isCrashedMutex.RLock()
	isCrashed := s.isCrashed
	s.isCrashedMutex.RUnlock()

	if (isCrashed) {
		return &Success{Flag: false}, ERR_SERVER_CRASHED
	}

	s.isLeaderMutex.RLock()
	isLeader := s.isLeader
	s.isLeaderMutex.RUnlock()

	if (!isLeader) {
		return &Success{Flag: false}, ERR_NOT_LEADER
	}

	majorityAlive := false
	aliveCount := 1
	for idx, addr := range s.ipList {
		if int64(idx) == s.serverId {
			continue
		}

		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		if err != nil {
			return &Success{Flag: false}, nil
		}

		client := NewRaftSurfstoreClient(conn)

		var prevLogTerm int64
		if s.commitIndex == -1 {
			prevLogTerm = 0
		} else {
			prevLogTerm = s.log[s.commitIndex].Term
		}

		input := &AppendEntryInput{
			Term:         s.term,
			PrevLogTerm:  prevLogTerm,
			PrevLogIndex: s.commitIndex,
			// TODO figure out which entries to send
			Entries:      s.log,
			LeaderCommit: s.commitIndex,
		}

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		c,_ := client.IsCrashed(context.Background(),&emptypb.Empty{})
		println("sending heartbeat to server: ",idx,"Server crashed ",c.IsCrashed)

		output, err := client.AppendEntries(ctx, input)
		if err != nil {
			continue
		}

		println("received heartbeat reply")
		if (output.Success) {
			aliveCount++
			if aliveCount > len(s.ipList)/2 {
				majorityAlive = true
			}
		}
	}
	return &Success{Flag: majorityAlive}, nil
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
