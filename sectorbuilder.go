package sectorbuilder

import (
	"container/list"
	"context"
	"fmt"
	"golang.org/x/exp/rand"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	sectorbuilder "github.com/filecoin-project/filecoin-ffi"
	"github.com/filecoin-project/go-address"
	datastore "github.com/ipfs/go-datastore"
	logging "github.com/ipfs/go-log"
	dcopy "github.com/otiai10/copy"
	"golang.org/x/xerrors"
)

const PoStReservedWorkers = 1
const PoRepProofPartitions = 10

var lastSectorIdKey = datastore.NewKey("/last")

var log = logging.Logger("sectorbuilder")

type SortedPublicSectorInfo = sectorbuilder.SortedPublicSectorInfo
type SortedPrivateSectorInfo = sectorbuilder.SortedPrivateSectorInfo

type SealTicket = sectorbuilder.SealTicket

type SealSeed = sectorbuilder.SealSeed

type SealPreCommitOutput = sectorbuilder.SealPreCommitOutput

type SealCommitOutput = sectorbuilder.SealCommitOutput

type PublicPieceInfo = sectorbuilder.PublicPieceInfo

type RawSealPreCommitOutput sectorbuilder.RawSealPreCommitOutput

type EPostCandidate = sectorbuilder.Candidate

const CommLen = sectorbuilder.CommitmentBytesLen

type WorkerCfg struct {
	NoSeal      bool
	NoPush      bool
	RemoteID    string

	// TODO: 'cost' info, probably in terms of sealing + transfer speed
}

var pushSectorNum = uint64(0)

type SectorBuilder struct {
	ds   datastore.Batching
	idLk sync.Mutex

	ssize  uint64
	lastID uint64

	Miner address.Address

	unsealLk sync.Mutex

	noCommit    bool
	noPreCommit bool
	rateLimit   chan struct{}


	sealTasks map[string]chan workerCall
	pushTasks map[string]chan workerCall

	taskCtr       uint64
	remoteLk      sync.Mutex
	remotes       map[string]*remote
	remoteResults map[uint64]chan<- SealRes

	addPieceWait  int32
	preCommitWait int32
	commitWait    int32
	unsealWait    int32

	fsLk       sync.Mutex //nolint: struckcheck
	filesystem *fs        // TODO: multi-fs support

	stopping chan struct{}

	pushLk        sync.Mutex
	pushDataQueue *list.List
}

type JsonRSPCO struct {
	CommD []byte
	CommR []byte
}

func (rspco *RawSealPreCommitOutput) ToJson() JsonRSPCO {
	return JsonRSPCO{
		CommD: rspco.CommD[:],
		CommR: rspco.CommR[:],
	}
}

func (rspco *JsonRSPCO) rspco() RawSealPreCommitOutput {
	var out RawSealPreCommitOutput
	copy(out.CommD[:], rspco.CommD)
	copy(out.CommR[:], rspco.CommR)
	return out
}

type SealRes struct {
	Err   string
	GoErr error `json:"-"`

	Proof []byte
	Rspco JsonRSPCO

	PieceCommp []byte
	RemoteID string
}

type remote struct {
	lk sync.Mutex

	sealTasks    chan<- WorkerTask
	remoteStatus WorkerTaskType //for control step

	//RemoteID
	RemoteID  string
}

type Config struct {
	SectorSize uint64
	Miner      address.Address

	WorkerThreads  uint8
	FallbackLastID uint64
	NoCommit       bool
	NoPreCommit    bool

	Dir string
	_   struct{} // guard against nameless init
}

func New(cfg *Config, ds datastore.Batching) (*SectorBuilder, error) {
	if cfg.WorkerThreads < PoStReservedWorkers {
		return nil, xerrors.Errorf("minimum worker threads is %d, specified %d", PoStReservedWorkers, cfg.WorkerThreads)
	}

	var lastUsedID uint64
	b, err := ds.Get(lastSectorIdKey)
	switch err {
	case nil:
		i, err := strconv.ParseInt(string(b), 10, 64)
		if err != nil {
			return nil, err
		}
		lastUsedID = uint64(i)
	case datastore.ErrNotFound:
		lastUsedID = cfg.FallbackLastID
	default:
		return nil, err
	}

	rlimit := cfg.WorkerThreads - PoStReservedWorkers

	//sealLocal := rlimit > 0

	if rlimit == 0 {
		rlimit = 1
	}

	sb := &SectorBuilder{
		ds: ds,

		ssize:  cfg.SectorSize,
		lastID: lastUsedID,

		filesystem: openFs(cfg.Dir),

		Miner: cfg.Miner,

		noPreCommit: true,
		noCommit:    true,
		rateLimit:   make(chan struct{}, rlimit),

		taskCtr:        1,

		sealTasks:     map[string]chan workerCall{},
		pushTasks:     map[string]chan workerCall{},
		remoteResults: map[uint64]chan<- SealRes{},
		remotes:       map[string]*remote{},

		stopping: make(chan struct{}),

		pushDataQueue: list.New(),
	}

	if err := sb.filesystem.init(); err != nil {
		return nil, xerrors.Errorf("initializing sectorbuilder filesystem: %w", err)
	}

	return sb, nil
}

func NewStandalone(cfg *Config) (*SectorBuilder, error) {
	sb := &SectorBuilder{
		ds: nil,

		ssize: cfg.SectorSize,

		Miner:      cfg.Miner,
		filesystem: openFs(cfg.Dir),

		taskCtr:       1,
		noCommit :     false,
		noPreCommit:   false,
		remotes:       map[string]*remote{},
		rateLimit:     make(chan struct{}, cfg.WorkerThreads),
		stopping:      make(chan struct{}),
		pushDataQueue: list.New(),
	}

	if err := sb.filesystem.init(); err != nil {
		return nil, xerrors.Errorf("initializing sectorbuilder filesystem: %w", err)
	}

	return sb, nil
}

func (sb *SectorBuilder) checkRateLimit() {
	if cap(sb.rateLimit) == len(sb.rateLimit) {
		log.Warn("rate-limiting local sectorbuilder call")
	}
}

func (sb *SectorBuilder) RateLimit() func() {
	sb.checkRateLimit()

	sb.rateLimit <- struct{}{}

	return func() {
		<-sb.rateLimit
	}
}

type WorkerStats struct {
	LocalFree     int
	LocalReserved int
	LocalTotal    int
	// todo: post in progress
	RemotesTotal int
	RemotesFree  int

	AddPieceWait  int
	PreCommitWait int
	PushDataWait  int
	CommitWait    int
	UnsealWait    int
}

func (sb *SectorBuilder) WorkerStats() WorkerStats {
	sb.remoteLk.Lock()
	defer sb.remoteLk.Unlock()

	remoteFree := len(sb.remotes)/2
	for _, r := range sb.remotes {
		if r.remoteStatus > WorkerIdle && (sb.sealTasks[r.RemoteID] != nil) {
			remoteFree--
		}
	}

	return WorkerStats{
		LocalFree:     cap(sb.rateLimit) - len(sb.rateLimit),
		LocalReserved: PoStReservedWorkers,
		LocalTotal:    cap(sb.rateLimit) + PoStReservedWorkers,
		RemotesTotal:  len(sb.remotes)/2,
		RemotesFree:   remoteFree,

		AddPieceWait:  int(atomic.LoadInt32(&sb.addPieceWait)),
		PreCommitWait: int(atomic.LoadInt32(&sb.preCommitWait)),
		PushDataWait:  sb.pushDataQueue.Len(),
		CommitWait:    int(atomic.LoadInt32(&sb.commitWait)),
		UnsealWait:    int(atomic.LoadInt32(&sb.unsealWait)),
	}
}

func addressToProverID(a address.Address) [32]byte {
	var proverId [32]byte
	copy(proverId[:], a.Payload())
	return proverId
}

func (sb *SectorBuilder) AcquireSectorId() (uint64, error) {
	sb.idLk.Lock()
	defer sb.idLk.Unlock()

	sb.lastID++
	id := sb.lastID

	err := sb.ds.Put(lastSectorIdKey, []byte(fmt.Sprint(id)))
	if err != nil {
		return 0, err
	}
	return id, nil
}

func (sb *SectorBuilder) SetRemoteStatus(remoteid string) (error) {
	if sb.remotes[remoteid] != nil {
		sb.remotes[remoteid].lk.Lock()
		sb.remotes[remoteid].remoteStatus = WorkerIdle
		sb.remotes[remoteid].lk.Unlock()
	}
	return nil
}

func (sb *SectorBuilder) AddPushData(id string) (error) {
	sb.pushDataQueue.PushFront(id)
	return nil
}

func (sb *SectorBuilder) AddPiece(pieceSize uint64, sectorId uint64, file io.Reader, existingPieceSizes []uint64, stagingPath string ) (PublicPieceInfo, error) {
	log.Infof("AddPiece SectorID: %d pieceSize:%d", sectorId, pieceSize)

	fs := sb.filesystem

	if err := fs.reserve(dataStaging, sb.ssize); err != nil {
		return PublicPieceInfo{}, err
	}
	defer fs.free(dataStaging, sb.ssize)

	f, werr, err := toReadableFile(file, int64(pieceSize))
	if err != nil {
		return PublicPieceInfo{}, err
	}


	stagedFile, err := sb.stagedSectorFile(sectorId)
    if stagingPath != "" {
	    stagedFile, err = os.OpenFile(stagingPath, os.O_RDWR|os.O_CREATE, 0777)
	}

	if err != nil {
		return PublicPieceInfo{}, err
	}

	_, _, commP, err := sectorbuilder.WriteWithAlignment(f, pieceSize, stagedFile, existingPieceSizes)
	if err != nil {
		return PublicPieceInfo{}, err
	}

	if err := stagedFile.Close(); err != nil {
		return PublicPieceInfo{}, err
	}

	if err := f.Close(); err != nil {
		return PublicPieceInfo{}, err
	}

	return PublicPieceInfo{
		Size:  pieceSize,
		CommP: commP,
	}, werr()
}

func (sb *SectorBuilder) sealAddPieceRemote(call workerCall) ([]byte,  string, error) {
	log.Info("sealAddPieceRemote...", "sectorID:", call.task.SectorID, "  RemoteID:", call.task.RemoteID)
	atomic.AddInt32(&sb.addPieceWait, -1)

	select {
	case ret := <-call.ret:
		var err error
		if ret.Err != "" {
			err = xerrors.New(ret.Err)
		}
		return ret.PieceCommp, ret.RemoteID, err
	case <-sb.stopping:
		return nil, "", xerrors.New("sectorbuilder stopped")
	}
}

//SealAddPieceLocal
func (sb *SectorBuilder) SealAddPieceLocal(sectorID uint64, size uint64, hostfix string) (pcommp[]byte, err error) {
	log.Info("SealAddPieceLocal...", "sectorID:", sectorID , " RemoteID:", hostfix)
	atomic.AddInt32(&sb.addPieceWait, -1)

	os.Mkdir(filepath.Join(os.TempDir(), ".lotus"), 0777)
	os.Mkdir(filepath.Join(os.TempDir(),  ".lotus", hostfix), 0777)

	var keyPath = filepath.Join(os.TempDir(), ".lotus", hostfix, ".lastcommP.dat")
	var piecePath = filepath.Join(os.TempDir(),".lotus",  hostfix, ".lastpiece.dat")

	//Check file
	fileinfo, err := os.Stat(sb.StagedSectorPath(sectorID))
	if err == nil || os.IsExist(err) {
		log.Warn("SealAddPieceLocal...", "sectorID:", sectorID , " RemoteID:", hostfix, " err:", err)
		os.Remove(sb.StagedSectorPath(sectorID))
	}

	pieceCommp, keyerr := ioutil.ReadFile(keyPath)

	pieceExist := false
	_, peiceerr := os.Stat(piecePath)
	if peiceerr == nil || os.IsExist(peiceerr) {
		pieceExist = true
	}
	if  len(pieceCommp) != 32 || !pieceExist || keyerr != nil  {
		ppi, err := sb.AddPiece(size, sectorID, io.LimitReader(rand.New(rand.NewSource(42)), int64(size)), []uint64{}, piecePath)
		if err != nil {
			log.Info("SealAddPieceLocal...", "sectorID:", sectorID , " RemoteID:", hostfix, " err", err)
			return nil, xerrors.Errorf("SealAddPieceLocal: %w", err)
		}
		err = ioutil.WriteFile(keyPath, ppi.CommP[:], 0777)
		if err != nil {
			return nil, xerrors.Errorf("SealAddPieceLocal WriteFile: %w", err)
		}

		pieceCommp = ppi.CommP[:]
		log.Info("SealAddPieceLocal...  ", "sb.AddPiece  sectorID:", sectorID, "  pieceCommp:", pieceCommp)
	}

	log.Info("SealAddPieceLocal...  ", " sectorID:", sectorID, "  pieceCommp:", pieceCommp)

	migrateFile(piecePath, sb.StagedSectorPath(sectorID), true)

	//Dobule Check
	fileinfo, err = os.Stat(sb.StagedSectorPath(sectorID))
	if err != nil || fileinfo.Size() == 0 {
		log.Warn("SealAddPieceLocal...", "sectorID:", sectorID , " RemoteID:", hostfix, " err:", err)
		os.Remove(sb.StagedSectorPath(sectorID))
		migrateFile(piecePath, sb.StagedSectorPath(sectorID), true)
	}

	return pieceCommp, nil
}

func (sb *SectorBuilder) sealPushDataRemote(call workerCall) (string, error) {
	defer func() {
		sb.pushLk.Lock()
		pushSectorNum = pushSectorNum - 1
		sb.pushLk.Unlock()
		go sb.DealPushData()
	}()

	log.Info("sealAddPieceRemote...", "sectorID:", call.task.SectorID, "  RemoteID:", call.task.RemoteID)
	sb.pushLk.Lock()
	pushSectorNum = pushSectorNum + 1
	sb.pushLk.Unlock()

	select {
	case ret := <-call.ret:
		var err error
		if ret.Err != "" {
			err = xerrors.New(ret.Err)
		}
		return ret.RemoteID, err
	case <-sb.stopping:
		return "", xerrors.New("sectorbuilder stopped")
	}
}

func (sb *SectorBuilder) DealPushData() (error) {
	key := os.Getenv("SEAL_PUSH_DATA_NUM")
	num, err := strconv.ParseUint(key, 10, 64)
	if err != nil || num == uint64(0) {
		num = 3
	}
	if pushSectorNum >= num {
		log.Infof("SealPushData... in process  pushSectorNum:%d num:%d ", pushSectorNum, num)
		return nil
	}

	if sb.pushDataQueue == nil || sb.pushDataQueue.Len() == 0 {
		return nil
	}

	var remoteID = ""
	var sectorID = uint64(0)
	var sector *list.Element
	lenth := sb.pushDataQueue.Len()
	for i := 0; i < lenth; i++ {
		ele := sb.pushDataQueue.Back()
		if ele == nil {
			log.Info("SealPushData... ele == nil  pushSectorNum:", pushSectorNum)
			return nil
		}

		value := ele.Value.(string)
		log.Info("SealPushData...", "pushDataQueue: ", value)
		ids := strings.Split(value,"-")
		tempremoteID := ids[0]
		tempsectorID, err := strconv.ParseUint(ids[1], 10, 64)

		if tempremoteID == ""  ||  tempsectorID == 0 {
			log.Error("SealPushData...", "remoteID: ", tempremoteID,  " sectorID: ",tempsectorID)
			sb.pushDataQueue.Remove(ele)
			continue
		}

		err = sb.CheckSector(tempsectorID)
		if err == nil {
			log.Info("SealPushData... Exist", " remoteID: ", tempremoteID,  " sectorID: ",tempsectorID)
			sb.pushDataQueue.Remove(ele)
			continue
		}

		{
			remoteID = tempremoteID
			sectorID = tempsectorID
			sector = ele
			log.Info("SealPushData...", "pushDataQueue:", sb.pushDataQueue.Len(), " worknum:", num," remoteID: ", remoteID,  " sectorID: ",sectorID)
			break
		}
	}

	log.Info("SealPushData...", "pushDataQueue:", sb.pushDataQueue.Len(), " worknum:", num," remoteID: ", remoteID,  " sectorID: ",sectorID)
	if remoteID == ""  ||  sectorID == 0 {
		log.Warn("SealPushData...", "remoteID: ", remoteID,  " sectorID: ",sectorID)
		return nil
	}

	//change RemoteID to pushtask
	remoteID = remoteID + ".push"
	call := workerCall{
		task: WorkerTask{
			Type:       WorkerPushData,
			TaskID:     atomic.AddUint64(&sb.taskCtr, 1),
			SectorID:   sectorID,
			RemoteID:   remoteID,
		},
		ret: make(chan SealRes),
	}

	task := sb.pushTasks[remoteID]
	if task == nil {
		log.Warn("SealPushData...", "remoteID: ", remoteID,  " sectorID: ",sectorID)
		return  xerrors.New("pushTasks not find")
	}

	sb.pushDataQueue.Remove(sector)

	select { // prefer remote
	case task <- call:
		 sb.sealPushDataRemote(call)
		 return nil
	default:
	}

	return nil
}

func (sb *SectorBuilder) SealAddPiece(ctx context.Context, sectorID uint64, remoteid string) ([]byte, string, error) {
	log.Info("SealAddPiece...", "sectorID: ", sectorID)
	call := workerCall{
		task: WorkerTask{
			Type:       WorkerAddPiece,
			TaskID:     atomic.AddUint64(&sb.taskCtr, 1),
			SectorID:   sectorID,
		},
		ret: make(chan SealRes),
	}

	if remoteid == "" {
		for _, r := range sb.remotes {
			if r.remoteStatus == WorkerIdle && sb.sealTasks[r.RemoteID] != nil {
				remoteid = r.RemoteID
				break
			}
		}
	}

	if remoteid == "" {
		return nil, "", xerrors.New("remoteid not find")
	}

    //TODO don't do this
	//if sb.sealTasks[remoteid] == nil {
	//	sb.sealTasks[remoteid] = make(chan workerCall)
	//}

	task := sb.sealTasks[remoteid]
	if task == nil {
		return nil, "", xerrors.New("sealTasks not find")
	}
	call.task.RemoteID = remoteid
	log.Info("SealAddPiece...", "RemoteID: ", remoteid)
	atomic.AddInt32(&sb.addPieceWait, 1)
	select { // prefer remote
	case task <- call:
		return sb.sealAddPieceRemote(call)
	default:
		select { // prefer remote
		case task <- call:
			log.Info("sealAddPieceRemote...", "sectorID: ", sectorID)
			return sb.sealAddPieceRemote(call)
		default:
			//rl := sb.rateLimit
			select { // use whichever is available
			case task <- call:
				log.Info("sealAddPieceRemote...", "sectorID:", sectorID)
				return sb.sealAddPieceRemote(call)
			case <-ctx.Done():
				return nil, "", ctx.Err()
			}
		}
	}

	return nil, "", xerrors.New("sectorbuilder stopped")
}

func (sb *SectorBuilder) ReadPieceFromSealedSector(sectorID uint64, offset uint64, size uint64, ticket []byte, commD []byte) (io.ReadCloser, error) {
	fs := sb.filesystem

	if err := fs.reserve(dataUnsealed, sb.ssize); err != nil { // TODO: this needs to get smarter when we start supporting partial unseals
		return nil, err
	}
	defer fs.free(dataUnsealed, sb.ssize)

	atomic.AddInt32(&sb.unsealWait, 1)
	// TODO: Don't wait if cached
	ret := sb.RateLimit() // TODO: check perf, consider remote unseal worker
	defer ret()
	atomic.AddInt32(&sb.unsealWait, -1)

	sb.unsealLk.Lock() // TODO: allow unsealing unrelated sectors in parallel
	defer sb.unsealLk.Unlock()

	cacheDir, err := sb.sectorCacheDir(sectorID)
	if err != nil {
		return nil, err
	}

	sealedPath, err := sb.SealedSectorPath(sectorID)
	if err != nil {
		return nil, err
	}

	unsealedPath := sb.unsealedSectorPath(sectorID)

	// TODO: GC for those
	//  (Probably configurable count of sectors to be kept unsealed, and just
	//   remove last used one (or use whatever other cache policy makes sense))
	f, err := os.OpenFile(unsealedPath, os.O_RDONLY, 0644)
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, err
		}

		var commd [CommLen]byte
		copy(commd[:], commD)

		var tkt [CommLen]byte
		copy(tkt[:], ticket)

		err = sectorbuilder.Unseal(sb.ssize,
			PoRepProofPartitions,
			cacheDir,
			sealedPath,
			unsealedPath,
			sectorID,
			addressToProverID(sb.Miner),
			tkt,
			commd)
		if err != nil {
			return nil, xerrors.Errorf("unseal failed: %w", err)
		}

		f, err = os.OpenFile(unsealedPath, os.O_RDONLY, 0644)
		if err != nil {
			return nil, err
		}
	}

	if _, err := f.Seek(int64(offset), io.SeekStart); err != nil {
		return nil, xerrors.Errorf("seek: %w", err)
	}

	lr := io.LimitReader(f, int64(size))

	return &struct {
		io.Reader
		io.Closer
	}{
		Reader: lr,
		Closer: f,
	}, nil
}

func (sb *SectorBuilder) sealPreCommitRemote(call workerCall) (RawSealPreCommitOutput, error) {
	log.Info("sealPreCommitRemote...", "sectorID:", call.task.SectorID)
	atomic.AddInt32(&sb.preCommitWait, -1)

	select {
	case ret := <-call.ret:
		var err error
		if ret.Err != "" {
			err = xerrors.New(ret.Err)
		} else {
			sb.AddPushData(call.task.RemoteID + "-" + strconv.Itoa(int(call.task.SectorID)))
			go sb.DealPushData()
		}
		return ret.Rspco.rspco(), err
	case <-sb.stopping:
		return RawSealPreCommitOutput{}, xerrors.New("sectorbuilder stopped")
	}
}

func (sb *SectorBuilder) SealPreCommitLocal(sectorID uint64, ticket SealTicket, pieces []PublicPieceInfo) (RawSealPreCommitOutput, error) {
	log.Info("SealPreCommitLocal...", "sectorID:", sectorID)
	atomic.AddInt32(&sb.preCommitWait, -1)
	fs := sb.filesystem

	if err := fs.reserve(dataCache, sb.ssize); err != nil {
		return RawSealPreCommitOutput{}, err
	}
	defer fs.free(dataCache, sb.ssize)

	if err := fs.reserve(dataSealed, sb.ssize); err != nil {
		return RawSealPreCommitOutput{}, err
	}
	defer fs.free(dataSealed, sb.ssize)

	cacheDir, err := sb.sectorCacheDir(sectorID)
	if err != nil {
		return RawSealPreCommitOutput{}, xerrors.Errorf("getting cache dir: %w", err)
	}

	sealedPath, err := sb.SealedSectorPath(sectorID)
	if err != nil {
		return RawSealPreCommitOutput{}, xerrors.Errorf("getting sealed sector path: %w", err)
	}

	e, err := os.OpenFile(sealedPath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return RawSealPreCommitOutput{}, xerrors.Errorf("ensuring sealed file exists: %w", err)
	}
	if err := e.Close(); err != nil {
		return RawSealPreCommitOutput{}, err
	}

	var sum uint64
	for _, piece := range pieces {
		sum += piece.Size
	}
	ussize := UserBytesForSectorSize(sb.ssize)
	if sum != ussize {
		return RawSealPreCommitOutput{}, xerrors.Errorf("aggregated piece sizes don't match sector size: %d != %d (%d)", sum, ussize, int64(ussize-sum))
	}

	stagedPath := sb.StagedSectorPath(sectorID)

	// TODO: context cancellation respect
	rspco, err := sectorbuilder.SealPreCommit(
		sb.ssize,
		PoRepProofPartitions,
		cacheDir,
		stagedPath,
		sealedPath,
		sectorID,
		addressToProverID(sb.Miner),
		ticket.TicketBytes,
		pieces,
	)
	if err != nil {
		return RawSealPreCommitOutput{}, xerrors.Errorf("presealing sector %d (%s): %w", sectorID, stagedPath, err)
	}

	log.Info("SealPreCommitLocal...", "PreCommitOutput:", sectorID)
	return RawSealPreCommitOutput(rspco), nil
}

func (sb *SectorBuilder) SealPreCommit(ctx context.Context, sectorID uint64, ticket SealTicket, pieces []PublicPieceInfo, remoteid string) (RawSealPreCommitOutput, error) {
	log.Info("SealPreCommit...", "RemoteID:", remoteid)

	if sb.sealTasks[remoteid] == nil {
		sb.sealTasks[remoteid] = make(chan workerCall)
	}

	specialtask := sb.sealTasks[remoteid]
	call := workerCall{
		task: WorkerTask{
			Type:       WorkerPreCommit,
			TaskID:     atomic.AddUint64(&sb.taskCtr, 1),
			SectorID:   sectorID,
			SealTicket: ticket,
			Pieces:     pieces,
			RemoteID:   remoteid,
		},
		ret: make(chan SealRes),
	}

	atomic.AddInt32(&sb.preCommitWait, 1)

	select { // prefer remote
	case specialtask <- call:
		return sb.sealPreCommitRemote(call)
	default:
		select { // use whichever is available
		case specialtask <- call:
			return sb.sealPreCommitRemote(call)
		case <-ctx.Done():
			return RawSealPreCommitOutput{}, ctx.Err()
		}
	}

	return RawSealPreCommitOutput{}, xerrors.New("sectorbuilder stopped")
}

func (sb *SectorBuilder) sealCommitRemote(call workerCall) (proof []byte, err error) {
	atomic.AddInt32(&sb.commitWait, -1)

	select {
	case ret := <-call.ret:
		if ret.Err != "" {
			err = xerrors.New(ret.Err)
		}
		return ret.Proof, err
	case <-sb.stopping:
		return nil, xerrors.New("sectorbuilder stopped")
	}
}

func (sb *SectorBuilder) SealCommitLocal(sectorID uint64, ticket SealTicket, seed SealSeed, pieces []PublicPieceInfo, rspco RawSealPreCommitOutput) (proof []byte, err error) {
	log.Info("SealCommitLocal...", "sectorID:", sectorID)
	atomic.AddInt32(&sb.commitWait, -1)

	cacheDir, err := sb.sectorCacheDir(sectorID)
	if err != nil {
		return nil, err
	}

	proof, err = sectorbuilder.SealCommit(
		sb.ssize,
		PoRepProofPartitions,
		cacheDir,
		sectorID,
		addressToProverID(sb.Miner),
		ticket.TicketBytes,
		seed.TicketBytes,
		pieces,
		sectorbuilder.RawSealPreCommitOutput(rspco),
	)
	if err != nil {
		log.Warn("StandaloneSealCommit error: ", err)
		log.Warnf("sid:%d tkt:%v seed:%v, ppi:%v rspco:%v", sectorID, ticket, seed, pieces, rspco)

		return nil, xerrors.Errorf("StandaloneSealCommit: %w", err)
	}

	log.Info("SealCommitLocal...end", "sectorID:", sectorID)
	return proof, nil
}

func (sb *SectorBuilder) SealCommit(ctx context.Context, sectorID uint64, ticket SealTicket, seed SealSeed, pieces []PublicPieceInfo, rspco RawSealPreCommitOutput, remoteid string) (proof []byte, err error) {
	log.Info("SealCommit...", "RemoteID:", remoteid)

	if sb.sealTasks[remoteid] == nil {
		sb.sealTasks[remoteid] = make(chan workerCall)
	}

	specialtask := sb.sealTasks[remoteid]

	call := workerCall{
		task: WorkerTask{
			Type:       WorkerCommit,
			TaskID:     atomic.AddUint64(&sb.taskCtr, 1),
			SectorID:   sectorID,
			SealTicket: ticket,
			Pieces:     pieces,

			SealSeed: seed,
			Rspco:    rspco,

			RemoteID: remoteid,
		},
		ret: make(chan SealRes),
	}

	atomic.AddInt32(&sb.commitWait, 1)

	err = sb.CheckSector(sectorID)
	if err != nil {
		sb.AddPushData(remoteid + "-" +  strconv.Itoa(int(sectorID)))
	}

	//TODO change to 40
	//if sb.pushDataQueue.Len() > 2 {
	//	return nil, xerrors.Errorf("PushDataQueueMax")
	//}

	select { // use whichever is available
	case specialtask <- call:
		log.Info("sealCommitRemote...", "RemoteID:", remoteid)
		proof, err = sb.sealCommitRemote(call)
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	if err != nil {
		return nil, xerrors.Errorf("commit: %w", err)
	}

	return proof, nil
}

func (sb *SectorBuilder) ComputeElectionPoSt(sectorInfo SortedPublicSectorInfo, challengeSeed []byte, winners []EPostCandidate) ([]byte, error) {
	if len(challengeSeed) != CommLen {
		return nil, xerrors.Errorf("given challenge seed was the wrong length: %d != %d", len(challengeSeed), CommLen)
	}
	var cseed [CommLen]byte
	copy(cseed[:], challengeSeed)

	privsects, err := sb.pubSectorToPriv(sectorInfo, nil) // TODO: faults
	if err != nil {
		return nil, err
	}

	proverID := addressToProverID(sb.Miner)

	return sectorbuilder.GeneratePoSt(sb.ssize, proverID, privsects, cseed, winners)
}

func (sb *SectorBuilder) GenerateEPostCandidates(sectorInfo SortedPublicSectorInfo, challengeSeed [CommLen]byte, faults []uint64) ([]EPostCandidate, error) {
	privsectors, err := sb.pubSectorToPriv(sectorInfo, faults)
	if err != nil {
		return nil, err
	}

	challengeCount := ElectionPostChallengeCount(uint64(len(sectorInfo.Values())), uint64(len(faults)))

	proverID := addressToProverID(sb.Miner)
	return sectorbuilder.GenerateCandidates(sb.ssize, proverID, challengeSeed, challengeCount, privsectors)
}

func (sb *SectorBuilder) pubSectorToPriv(sectorInfo SortedPublicSectorInfo, faults []uint64) (SortedPrivateSectorInfo, error) {
	fmap := map[uint64]struct{}{}
	for _, fault := range faults {
		fmap[fault] = struct{}{}
	}

	var out []sectorbuilder.PrivateSectorInfo
	for _, s := range sectorInfo.Values() {
		if _, faulty := fmap[s.SectorID]; faulty {
			continue
		}

		cachePath, err := sb.sectorCacheDir(s.SectorID)
		if err != nil {
			return SortedPrivateSectorInfo{}, xerrors.Errorf("getting cache path for sector %d: %w", s.SectorID, err)
		}

		sealedPath, err := sb.SealedSectorPath(s.SectorID)
		if err != nil {
			return SortedPrivateSectorInfo{}, xerrors.Errorf("getting sealed path for sector %d: %w", s.SectorID, err)
		}

		out = append(out, sectorbuilder.PrivateSectorInfo{
			SectorID:         s.SectorID,
			CommR:            s.CommR,
			CacheDirPath:     cachePath,
			SealedSectorPath: sealedPath,
		})
	}
	return newSortedPrivateSectorInfo(out), nil
}

func (sb *SectorBuilder) GenerateFallbackPoSt(sectorInfo SortedPublicSectorInfo, challengeSeed [CommLen]byte, faults []uint64) ([]EPostCandidate, []byte, error) {
	privsectors, err := sb.pubSectorToPriv(sectorInfo, faults)
	if err != nil {
		return nil, nil, err
	}

	challengeCount := fallbackPostChallengeCount(uint64(len(sectorInfo.Values())), uint64(len(faults)))

	proverID := addressToProverID(sb.Miner)
	candidates, err := sectorbuilder.GenerateCandidates(sb.ssize, proverID, challengeSeed, challengeCount, privsectors)
	if err != nil {
		return nil, nil, err
	}

	proof, err := sectorbuilder.GeneratePoSt(sb.ssize, proverID, privsectors, challengeSeed, candidates)
	return candidates, proof, err
}

func (sb *SectorBuilder) Stop() {
	close(sb.stopping)
}

func fallbackPostChallengeCount(sectors uint64, faults uint64) uint64 {
	challengeCount := ElectionPostChallengeCount(sectors, faults)
	if challengeCount > MaxFallbackPostChallengeCount {
		return MaxFallbackPostChallengeCount
	}
	return challengeCount
}

func (sb *SectorBuilder) ImportFrom(osb *SectorBuilder, symlink bool) error {
	if err := migrate(osb.filesystem.pathFor(dataCache), sb.filesystem.pathFor(dataCache), symlink); err != nil {
		return err
	}

	if err := migrate(osb.filesystem.pathFor(dataStaging), sb.filesystem.pathFor(dataStaging), symlink); err != nil {
		return err
	}

	if err := migrate(osb.filesystem.pathFor(dataSealed), sb.filesystem.pathFor(dataSealed), symlink); err != nil {
		return err
	}

	val, err := osb.ds.Get(lastSectorIdKey)
	if err != nil {
		if err == datastore.ErrNotFound {
			log.Warnf("CAUTION: last sector ID not found in previous datastore")
			return nil
		}
		return err
	}

	if err := sb.ds.Put(lastSectorIdKey, val); err != nil {
		return err
	}

	sb.lastID = osb.lastID

	return nil
}

func (sb *SectorBuilder) SetLastSectorID(id uint64) error {
	if err := sb.ds.Put(lastSectorIdKey, []byte(fmt.Sprint(id))); err != nil {
		return err
	}

	sb.lastID = id
	return nil
}

func migrate(from, to string, symlink bool) error {
	st, err := os.Stat(from)
	if err != nil {
		return err
	}

	if st.IsDir() {
		return migrateDir(from, to, symlink)
	}
	return migrateFile(from, to, symlink)
}

func migrateDir(from, to string, symlink bool) error {
	tost, err := os.Stat(to)
	if err != nil {
		if !os.IsNotExist(err) {
			return err
		}

		if err := os.MkdirAll(to, 0755); err != nil {
			return err
		}
	} else if !tost.IsDir() {
		return xerrors.Errorf("target %q already exists and is a file (expected directory)")
	}

	dirents, err := ioutil.ReadDir(from)
	if err != nil {
		return err
	}

	for _, inf := range dirents {
		n := inf.Name()
		if inf.IsDir() {
			if err := migrate(filepath.Join(from, n), filepath.Join(to, n), symlink); err != nil {
				return err
			}
		} else {
			if err := migrate(filepath.Join(from, n), filepath.Join(to, n), symlink); err != nil {
				return err
			}
		}
	}

	return nil
}

func migrateFile(from, to string, symlink bool) error {
	if symlink {
		return os.Symlink(from, to)
	}

	return dcopy.Copy(from, to)
}
