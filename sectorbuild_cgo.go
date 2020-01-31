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

	fs "github.com/filecoin-project/go-sectorbuilder/fs"
)

var _ Interface = &SectorBuilder{}

func (sb *SectorBuilder) AddPiece(ctx context.Context, pieceSize uint64, sectorId uint64, file io.Reader, existingPieceSizes []uint64, stagingPath string) (PublicPieceInfo, error) {
	//atomic.AddInt32(&sb.addPieceWait, 1)
	//ret := sb.RateLimit()
	//atomic.AddInt32(&sb.addPieceWait, -1)
	//defer ret()

	f, werr, err := toReadableFile(file, int64(pieceSize))
	if err != nil {
		return PublicPieceInfo{}, err
	}

	var stagedFile *os.File
	var stagedPath fs.SectorPath
	if len(existingPieceSizes) == 0 {
		if stagingPath != "" {
			stagedPath = fs.SectorPath(stagingPath)
		} else  {
			stagedPath, err = sb.AllocSectorPath(fs.DataStaging, sectorId, true)
			if err != nil {
				return PublicPieceInfo{}, xerrors.Errorf("allocating sector: %w", err)
			}
		}

		stagedFile, err = os.Create(string(stagedPath))
		if err != nil {
			return PublicPieceInfo{}, xerrors.Errorf("opening sector file: %w", err)
		}

		defer sb.filesystem.Release(stagedPath, sb.ssize)
	} else {
		if stagingPath != "" {
			stagedPath = fs.SectorPath(stagingPath)
		} else {
			stagedPath, err = sb.SectorPath(fs.DataStaging, sectorId)
			if err != nil {
				return PublicPieceInfo{}, xerrors.Errorf("getting sector path: %w", err)
			}
		}

		stagedFile, err = os.OpenFile(string(stagedPath), os.O_RDWR, 0644)
		if err != nil {
			return PublicPieceInfo{}, xerrors.Errorf("opening sector file: %w", err)
		}
	}

	if err := sb.filesystem.Lock(ctx, stagedPath); err != nil {
		return PublicPieceInfo{}, err
	}
	defer sb.filesystem.Unlock(stagedPath)

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

func (sb *SectorBuilder) ReadPieceFromSealedSector(ctx context.Context, sectorID uint64, offset uint64, size uint64, ticket []byte, commD []byte) (io.ReadCloser, error) {
	sfs := sb.filesystem

	// TODO: this needs to get smarter when we start supporting partial unseals
	unsealedPath, err := sfs.AllocSector(fs.DataUnsealed, sb.Miner, sb.ssize, true, sectorID)
	if err != nil {
		if !xerrors.Is(err, fs.ErrExists) {
			return nil, xerrors.Errorf("AllocSector: %w", err)
		}
	}
	defer sfs.Release(unsealedPath, sb.ssize)

	if err := sfs.Lock(ctx, unsealedPath); err != nil {
		return nil, err
	}
	defer sfs.Unlock(unsealedPath)

	atomic.AddInt32(&sb.unsealWait, 1)
	// TODO: Don't wait if cached
	ret := sb.RateLimit() // TODO: check perf, consider remote unseal worker
	defer ret()
	atomic.AddInt32(&sb.unsealWait, -1)

	sb.unsealLk.Lock() // TODO: allow unsealing unrelated sectors in parallel
	defer sb.unsealLk.Unlock()

	cacheDir, err := sb.SectorPath(fs.DataCache, sectorID)
	if err != nil {
		return nil, err
	}

	sealedPath, err := sb.SectorPath(fs.DataSealed, sectorID)
	if err != nil {
		return nil, err
	}

	// TODO: GC for those
	//  (Probably configurable count of sectors to be kept unsealed, and just
	//   remove last used one (or use whatever other cache policy makes sense))
	f, err := os.OpenFile(string(unsealedPath), os.O_RDONLY, 0644)
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
			string(cacheDir),
			string(sealedPath),
			string(unsealedPath),
			sectorID,
			addressToProverID(sb.Miner),
			tkt,
			commd)
		if err != nil {
			return nil, xerrors.Errorf("unseal failed: %w", err)
		}

		f, err = os.OpenFile(string(unsealedPath), os.O_RDONLY, 0644)
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

func (sb *SectorBuilder) SealPreCommitLocal(ctx context.Context, sectorID uint64, ticket SealTicket, pieces []PublicPieceInfo) (RawSealPreCommitOutput, error) {
	log.Info("SealPreCommitLocal...", "sectorID:", sectorID)
	sfs := sb.filesystem

	cacheDir, err := sfs.ForceAllocSector(fs.DataCache, sb.Miner, sb.ssize, true, sectorID)
	if err != nil {
		return RawSealPreCommitOutput{}, xerrors.Errorf("getting cache dir: %w", err)
	}
	if err := os.Mkdir(string(cacheDir), 0755); err != nil {
		return RawSealPreCommitOutput{}, err
	}

	sealedPath, err := sfs.ForceAllocSector(fs.DataSealed, sb.Miner, sb.ssize, true, sectorID)
	if err != nil {
		return RawSealPreCommitOutput{}, xerrors.Errorf("getting sealed sector paths: %w", err)
	}

	defer sfs.Release(cacheDir, sb.ssize)
	defer sfs.Release(sealedPath, sb.ssize)

	if err := sfs.Lock(ctx, cacheDir); err != nil {
		return RawSealPreCommitOutput{}, xerrors.Errorf("lock cache: %w", err)
	}
	if err := sfs.Lock(ctx, sealedPath); err != nil {
		return RawSealPreCommitOutput{}, xerrors.Errorf("lock sealed: %w", err)
	}
	defer sfs.Unlock(cacheDir)
	defer sfs.Unlock(sealedPath)
	sb.checkRateLimit()

	e, err := os.OpenFile(string(sealedPath), os.O_RDWR|os.O_CREATE, 0644)
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

	stagedPath, err := sb.SectorPath(fs.DataStaging, sectorID)
	if err != nil {
		return RawSealPreCommitOutput{}, xerrors.Errorf("get staged: %w", err)
	}
	// TODO: context cancellation respect
	rspco, err := ffi.SealPreCommit(
		sb.ssize,
		PoRepProofPartitions,
		string(cacheDir),
		string(stagedPath),
		string(sealedPath),
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

func (sb *SectorBuilder) SealCommitLocal(ctx context.Context, sectorID uint64, ticket SealTicket, seed SealSeed, pieces []PublicPieceInfo, rspco RawSealPreCommitOutput) (proof []byte, err error) {
	atomic.AddInt32(&sb.commitWait, -1)

	//defer func() {
	//	<-sb.rateLimit
	//}()

	cacheDir, err := sb.SectorPath(fs.DataCache, sectorID)
	if err != nil {
		return nil, err
	}
	if err := sb.filesystem.Lock(ctx, cacheDir); err != nil {
		return nil, err
	}
	defer sb.filesystem.Unlock(cacheDir)

	proof, err = ffi.SealCommit(
		sb.ssize,
		PoRepProofPartitions,
		string(cacheDir),
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
	spath, _ :=sb.SectorPath(fs.DataStaging, sectorID )
	fileinfo, err := os.Stat(string(spath))
	if err == nil || os.IsExist(err) {
		log.Warn("SealAddPieceLocal...", "sectorID:", sectorID , " RemoteID:", hostfix, " err:", err)
		os.Remove(string(spath))
	}

	pieceCommp, keyerr := ioutil.ReadFile(keyPath)

	pieceExist := false
	_, peiceerr := os.Stat(piecePath)
	if peiceerr == nil || os.IsExist(peiceerr) {
		pieceExist = true
	}
	if  len(pieceCommp) != 32 || !pieceExist || keyerr != nil  {
		ppi, err := sb.AddPiece(context.TODO(), size, sectorID, io.LimitReader(rand.New(rand.NewSource(42)), int64(size)), []uint64{}, piecePath)
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

	os.Symlink(piecePath, string(spath))

	//Dobule Check
	fileinfo, err = os.Stat(string(spath))
	if err != nil || fileinfo.Size() == 0 {
		log.Warn("SealAddPieceLocal...", "sectorID:", sectorID , " RemoteID:", hostfix, " err:", err)
		os.Remove(string(spath))
		os.Symlink(piecePath, string(spath))
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
