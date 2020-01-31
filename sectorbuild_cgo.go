//+build cgo

package sectorbuilder

import (
	"context"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"sync/atomic"

	ffi "github.com/filecoin-project/filecoin-ffi"
	"golang.org/x/xerrors"
)

var pushSectorNum = uint64(0)

var _ Interface = &SectorBuilder{}

func (sb *SectorBuilder) AddPiece(pieceSize uint64, sectorId uint64, file io.Reader, existingPieceSizes []uint64, stagingPath string ) (PublicPieceInfo, error) {
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

	_, _, commP, err := ffi.WriteWithAlignment(f, pieceSize, stagedFile, existingPieceSizes)
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

		err = ffi.Unseal(sb.ssize,
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

func (sb *SectorBuilder) SealPreCommit(ctx context.Context, sectorID uint64, ticket SealTicket, pieces []PublicPieceInfo, remoteid string) (RawSealPreCommitOutput, error) {


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
	rspco, err := ffi.SealPreCommit(
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

	return RawSealPreCommitOutput(rspco), nil
}

func (sb *SectorBuilder) SealCommitLocal(sectorID uint64, ticket SealTicket, seed SealSeed, pieces []PublicPieceInfo, rspco RawSealPreCommitOutput) (proof []byte, err error) {
	atomic.AddInt32(&sb.commitWait, -1)

	cacheDir, err := sb.sectorCacheDir(sectorID)
	if err != nil {
		return nil, err
	}

	proof, err = ffi.SealCommit(
		sb.ssize,
		PoRepProofPartitions,
		cacheDir,
		sectorID,
		addressToProverID(sb.Miner),
		ticket.TicketBytes,
		seed.TicketBytes,
		pieces,
		ffi.RawSealPreCommitOutput(rspco),
	)
	if err != nil {
		log.Warn("StandaloneSealCommit error: ", err)
		log.Warnf("sid:%d tkt:%v seed:%v, ppi:%v rspco:%v", sectorID, ticket, seed, pieces, rspco)

		return nil, xerrors.Errorf("StandaloneSealCommit: %w", err)
	}

	return proof, nil
}

func (sb *SectorBuilder) SealCommit(ctx context.Context, sectorID uint64, ticket SealTicket, seed SealSeed, pieces []PublicPieceInfo, rspco RawSealPreCommitOutput, remoteid string, storagepath string) (proof []byte, err error) {

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

	err = sb.CheckSector("", sectorID)
	if err != nil {
		sb.AddPushData(PushData{RemoteID: remoteid, SectorID: sectorID, StoragePath:storagepath,})
		go sb.DealPushData(nil)
	}

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

	return ffi.GeneratePoSt(sb.ssize, proverID, privsects, cseed, winners)
}

func (sb *SectorBuilder) GenerateEPostCandidates(sectorInfo SortedPublicSectorInfo, challengeSeed [CommLen]byte, faults []uint64) ([]EPostCandidate, error) {
	privsectors, err := sb.pubSectorToPriv(sectorInfo, faults)
	if err != nil {
		return nil, err
	}

	challengeCount := ElectionPostChallengeCount(uint64(len(sectorInfo.Values())), uint64(len(faults)))

	proverID := addressToProverID(sb.Miner)
	return ffi.GenerateCandidates(sb.ssize, proverID, challengeSeed, challengeCount, privsectors)
}

func (sb *SectorBuilder) GenerateFallbackPoSt(sectorInfo SortedPublicSectorInfo, challengeSeed [CommLen]byte, faults []uint64) ([]EPostCandidate, []byte, error) {
	privsectors, err := sb.pubSectorToPriv(sectorInfo, faults)
	if err != nil {
		return nil, nil, err
	}

	challengeCount := fallbackPostChallengeCount(uint64(len(sectorInfo.Values())), uint64(len(faults)))

	proverID := addressToProverID(sb.Miner)
	candidates, err := ffi.GenerateCandidates(sb.ssize, proverID, challengeSeed, challengeCount, privsectors)
	if err != nil {
		return nil, nil, err
	}

	proof, err := ffi.GeneratePoSt(sb.ssize, proverID, privsectors, challengeSeed, candidates)
	return candidates, proof, err
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
		//ppi, err := sb.AddPiece(size, sectorID, sb.pledgeReader(size, uint64(runtime.NumCPU())),  []uint64{}, piecePath)
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

	//log.Info("SealAddPieceLocal...  ", " sectorID:", sectorID, "  pieceCommp:", pieceCommp)

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

	select { // use whichever is available
	case task <- call:
		log.Info("sealAddPieceRemote...", "sectorID:", sectorID)
		return sb.sealAddPieceRemote(call)
	case <-ctx.Done():
		return nil, "", ctx.Err()
	}


	return nil, "", xerrors.New("sectorbuilder stopped")
}
