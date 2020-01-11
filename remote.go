package sectorbuilder

import (
	"context"

	"golang.org/x/xerrors"
)

type WorkerTaskType int

const (
	WorkerIdle WorkerTaskType = iota
	WorkerAddPiece
	WorkerPreCommit
	WorkerCommit
	WorkerPushData
)

type WorkerTask struct {
	Type   WorkerTaskType
	TaskID uint64

	SectorID uint64

	// AddPiece
	CommP []byte

	// preCommit
	SealTicket SealTicket
	Pieces     []PublicPieceInfo

	// commit
	SealSeed SealSeed
	Rspco    RawSealPreCommitOutput

	// remoteid
	RemoteID string
}

type workerCall struct {
	task WorkerTask
	ret  chan SealRes
}

func (sb *SectorBuilder) AddWorker(ctx context.Context, cfg WorkerCfg) (<-chan WorkerTask, error) {
	sb.remoteLk.Lock()
	defer sb.remoteLk.Unlock()

	taskCh := make(chan WorkerTask)
	r := &remote{
		sealTasks:    taskCh,
		remoteStatus: WorkerIdle,
		RemoteID:     cfg.RemoteID,
	}

	sb.remotes[cfg.RemoteID] = r

	go sb.remoteWorker(ctx, r, cfg)

	return taskCh, nil
}

func (sb *SectorBuilder) returnTask(task workerCall) {
	var ret chan workerCall
	switch task.task.Type {
	case WorkerAddPiece:
	case WorkerPreCommit:
	case WorkerCommit:
		remoteid := task.task.RemoteID
		log.Info("returnTask...", "RemoteID:", remoteid, "  type:",  task.task.Type)
		ret = sb.sealTasks[remoteid]
	case WorkerPushData:
		remoteid := task.task.RemoteID
		log.Info("returnTask...", "RemoteID:", remoteid, "  type:",  task.task.Type)
		ret = sb.pushTasks[remoteid]
	default:
		log.Error("unknown task type", task.task.Type)
	}

	go func() {
		select {
		case ret <- task:
		case <-sb.stopping:
			return
		}
	}()
}

func (sb *SectorBuilder) remoteWorker(ctx context.Context, r *remote, cfg WorkerCfg) {
	defer log.Warn("Remote worker disconnected")

	defer func() {
		sb.remoteLk.Lock()
		defer sb.remoteLk.Unlock()

		for i, vr := range sb.remotes {
			if vr == r {
				sb.sealTasks[r.RemoteID] = nil
				sb.pushTasks[r.RemoteID] = nil
				delete(sb.remotes, i)
				return
			}
		}
	}()

	log.Infof("remoteWorker WorkerCfg RemoteID: %s", cfg.RemoteID)

	if !cfg.NoSeal && cfg.RemoteID != "" && sb.sealTasks[cfg.RemoteID] == nil {
		sb.sealTasks[cfg.RemoteID] = make(chan workerCall)
		r.RemoteID = cfg.RemoteID
		log.Infof("sb.sealTasks make RemoteID: %s", cfg.RemoteID)
	}

	if !cfg.NoPush && cfg.RemoteID != "" && sb.pushTasks[cfg.RemoteID] == nil {
		sb.pushTasks[cfg.RemoteID] = make(chan workerCall)
		r.RemoteID = cfg.RemoteID
		log.Infof("sb.pushTasks make RemoteID: %s", cfg.RemoteID)
	}

	seal := sb.sealTasks[cfg.RemoteID]
	if cfg.NoSeal {
		seal = nil
	}
	push := sb.pushTasks[cfg.RemoteID]
	if cfg.NoPush {
		push = nil
	}

	for {
		select {
		// prefer sealTasks
		case task := <- seal:
			log.Infof("sealTasks SectorID: %d Type: %d RemoteID: %s", task.task.SectorID, task.task.Type, task.task.RemoteID)
			sb.doTask(ctx, r, task)
		case task := <- push:
			log.Infof("pushTasks SectorID: %d Type: %d RemoteID: %s", task.task.SectorID, task.task.Type, task.task.RemoteID)
			sb.doTask(ctx, r, task)
		case <-ctx.Done():
			return
		case <-sb.stopping:
			return
		}


		r.lk.Lock()
		if r.remoteStatus == WorkerCommit {
			r.remoteStatus = WorkerIdle
		}
		r.lk.Unlock()
	}
}

func (sb *SectorBuilder) doTask(ctx context.Context, r *remote, task workerCall) {
	resCh := make(chan SealRes)

	sb.remoteLk.Lock()
	sb.remoteResults[task.task.TaskID] = resCh
	sb.remoteLk.Unlock()

	// send the task
	select {
	case r.sealTasks <- task.task:
	case <-ctx.Done():
		sb.returnTask(task)
		return
	}

	r.lk.Lock()
	r.remoteStatus = task.task.Type
	r.lk.Unlock()

	// wait for the result
	select {
	case res := <-resCh:

		// send the result back to the caller
		select {
		case task.ret <- res:
		case <-ctx.Done():
			return
		case <-sb.stopping:
			return
		}

	case <-ctx.Done():
		log.Warnf("context expired while waiting for task %d (sector %d): %s", task.task.TaskID, task.task.SectorID, ctx.Err())
		return
	case <-sb.stopping:
		return
	}
}

func (sb *SectorBuilder) TaskDone(ctx context.Context, task uint64,  remoteid string, res SealRes) error {
	sb.remoteLk.Lock()
	rres, ok := sb.remoteResults[task]
	if ok {
		delete(sb.remoteResults, task)
	}
	sb.remoteLk.Unlock()

	if !ok {
		sb.remotes[remoteid].remoteStatus = WorkerIdle
		return xerrors.Errorf("task %d not found", task)
	}

	if res.GoErr != nil {
		log.Error("TaskDone GoErr RemoteID: %s", remoteid)
		sb.remotes[remoteid].remoteStatus = WorkerIdle
	}

	select {
	case rres <- res:
		return nil
	case <-ctx.Done():
		err := ctx.Err()
		if err != nil {
			log.Error("TaskDone GoErr RemoteID: %s", remoteid)
			sb.remotes[remoteid].remoteStatus = WorkerIdle
		}
		return err
	}
}
