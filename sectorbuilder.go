package sectorbuilder

import (
	"container/list"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"

	ffi "github.com/filecoin-project/filecoin-ffi"
	"github.com/filecoin-project/go-address"
	"github.com/ipfs/go-datastore"
	logging "github.com/ipfs/go-log"
	dcopy "github.com/otiai10/copy"
	"golang.org/x/xerrors"
)

const PoStReservedWorkers = 1
const PoRepProofPartitions = 10

var lastSectorIdKey = datastore.NewKey("/last")

var log = logging.Logger("sectorbuilder")

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
		pushDataQueue: nil,
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
func (sb *SectorBuilder) SaveStoragePath(key uint64, storagepath string) error {
	if key !=  0 {
		log.Infof("SaveStoragePath %d=%s", key, storagepath)
		sb.storageMap.Store(key, storagepath)
		return nil
	}
	//if err := sb.ds.Put(datastore.NewKey(key), storagepath); err != nil {
	//	log.Error("sealCommitRemote...", " SectorID:", key, "  StoragePath:", storagepath)
	//	return err
	//}
	log.Warn("SaveStoragePath  key is nil")
	return nil
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

	key := os.Getenv("SEAL_ID_INDEX")
	num, err := strconv.ParseUint(key, 10, 64)
	if err != nil || num == uint64(0) {
		num = 0
	}

	id = id + num

	return id, nil
}

func (sb *SectorBuilder) SetRemoteStatus(remoteid string ,status WorkerTaskType) (error) {
	if sb.remotes[remoteid] != nil {
		sb.remotes[remoteid].lk.Lock()
		sb.remotes[remoteid].remoteStatus = status
		sb.remotes[remoteid].lk.Unlock()
	}
	return nil
}
func (sb *SectorBuilder) sealPreCommitRemote(call workerCall) (RawSealPreCommitOutput, error) {
	atomic.AddInt32(&sb.preCommitWait, -1)

	select {
	case ret := <-call.ret:
		var err error
		if ret.Err != "" {
			err = xerrors.New(ret.Err)
		}
		return ret.Rspco.rspco(), err
	case <-sb.stopping:
		return RawSealPreCommitOutput{}, xerrors.New("sectorbuilder stopped")
	}
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

func (sb *SectorBuilder) pubSectorToPriv(sectorInfo SortedPublicSectorInfo, faults []uint64) (SortedPrivateSectorInfo, error) {
	fmap := map[uint64]struct{}{}
	for _, fault := range faults {
		fmap[fault] = struct{}{}
	}

	var out []ffi.PrivateSectorInfo
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

		out = append(out, ffi.PrivateSectorInfo{
			SectorID:         s.SectorID,
			CommR:            s.CommR,
			CacheDirPath:     cachePath,
			SealedSectorPath: sealedPath,
		})
	}
	return ffi.NewSortedPrivateSectorInfo(out...), nil
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

func (sb *SectorBuilder) Stop() {
	close(sb.stopping)
}

func (sb *SectorBuilder) DealPushData(ids interface{}) (error) {
	key := os.Getenv("SEAL_PUSH_DATA_NUM")
	num, err := strconv.ParseUint(key, 10, 64)
	if err != nil || num == uint64(0) {
		num = 3
	}
	var remoteID = ""
	var storagePath = ""
	var sectorID = uint64(0)
	var sector *list.Element = nil

	if ids == nil {
		if pushSectorNum >= num {
			log.Infof("SealPushData... in process  pushSectorNum:%d num:%d ", pushSectorNum, num)
			return nil
		}

		if sb.pushDataQueue == nil || sb.pushDataQueue.Len() == 0 {
			return nil
		}

		lenth := sb.pushDataQueue.Len()
		for i := 0; i < lenth; i++ {
			ele := sb.pushDataQueue.Back()
			if ele == nil {
				log.Info("SealPushData... ele == nil  pushSectorNum:", pushSectorNum)
				return nil
			}

			data := ele.Value.(PushData)
			tempremoteID := data.RemoteID
			tempsectorID := data.SectorID
			tempstoragePath := data.StoragePath

			if tempremoteID == "" || tempsectorID == 0 {
				log.Error("SealPushData...", "remoteID: ", tempremoteID, " sectorID: ", tempsectorID)
				sb.pushDataQueue.Remove(ele)
				continue
			}

			pushremoteID := tempremoteID + ".push"
			task := sb.pushTasks[pushremoteID]
			if task == nil {
				log.Warn("SealPushData...pushTasks is nil  ", "remoteID: ", pushremoteID, " sectorID: ", tempsectorID)
				//sb.pushDataQueue.MoveToFront(ele)
				sb.pushDataQueue.Remove(ele)
				continue
			}

			err = sb.CheckSector("", tempsectorID)
			if err == nil {
				log.Info("SealPushData... Exist", " remoteID: ", tempremoteID, " sectorID: ", tempsectorID)
				sb.pushDataQueue.Remove(ele)
				continue
			}

			remoteID = pushremoteID
			storagePath = tempstoragePath
			sectorID = tempsectorID

			sector = ele
			break
		}
	} else {
		tmpid := ids.(PushData)
		log.Info("SealPushData...", tmpid)
		for item := sb.pushDataQueue.Front();nil != item ;item = item.Next() {
			data := item.Value.(PushData)
			tempremoteID := data.RemoteID
			tempsectorID := data.SectorID
			tempstoragePath := data.StoragePath
			if  tempremoteID == tmpid.RemoteID &&  tempsectorID == tmpid.SectorID && tempstoragePath == tmpid.StoragePath {
				log.Info("SealPushData...", data)
				return nil
			}
		}

		return xerrors.New("pushDataQueue Tasks not find")
	}

	log.Infof("SealPushData... %d/%d %s %d %s", sb.pushDataQueue.Len(), pushSectorNum, remoteID, sectorID, storagePath)
	if remoteID == ""  ||  sectorID == 0 {
		return nil
	}

	//change RemoteID to pushtask
	pushcall := workerCall{
		task: WorkerTask{
			Type:       WorkerPushData,
			TaskID:     atomic.AddUint64(&sb.taskCtr, 1),
			SectorID:   sectorID,
			RemoteID:   remoteID,
			StoragePath: storagePath,
		},
		ret: make(chan SealRes),
	}

	pushtask := sb.pushTasks[remoteID]
	if pushtask == nil {
		log.Warn("SealPushData...", "remoteID: ", remoteID,  " sectorID: ",sectorID)
		return  xerrors.New("pushTasks not find")
	}

	if sector != nil {
		sb.pushDataQueue.Remove(sector)
	}

	select { // prefer remote
	case pushtask <- pushcall:
		sb.sealPushDataRemote(pushcall)
		return nil
	default:
	}

	return nil
}
func (sb *SectorBuilder) AddPushData(id interface{}) (error) {
	sb.pushDataQueue.PushFront(id)
	return nil
}

func (sb *SectorBuilder) pledgeReader(size uint64, parts uint64) io.Reader {
	piece := UserBytesForSectorSize((size/127 + size) / parts)

	readers := make([]io.Reader, parts)
	for i := range readers {
		readers[i] = io.LimitReader(rand.New(rand.NewSource(42+int64(i))), int64(piece))
	}

	return io.MultiReader(readers...)
}

func (sb *SectorBuilder) sealPushDataRemote(call workerCall) (string, error) {
	defer func() {
		sb.pushLk.Lock()
		pushSectorNum = pushSectorNum - 1
		sb.pushLk.Unlock()
		go sb.DealPushData(nil)
	}()

	log.Info("sealPushDataRemote...", "sectorID:", call.task.SectorID, "  RemoteID:", call.task.RemoteID)
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
