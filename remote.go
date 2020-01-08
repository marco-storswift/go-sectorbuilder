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
		ret = sb.specialcommitTasks[remoteid]
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
				sb.specialcommitTasks[r.RemoteID] = nil
				delete(sb.remotes, i)
				return
			}
		}
	}()

	log.Infof("remoteWorker WorkerCfg RemoteID: %s", cfg.RemoteID)

	if cfg.RemoteID != "" && sb.specialcommitTasks[cfg.RemoteID] == nil {
		sb.specialcommitTasks[cfg.RemoteID] = make(chan workerCall)
		r.RemoteID = cfg.RemoteID
		log.Infof("sb.specialcommitTasks make RemoteID: %s", cfg.RemoteID)
	}

	for {
		select {
		// prefer specialcommitTasks
		case task := <-sb.specialcommitTasks[cfg.RemoteID]:
			log.Infof("specialcommitTasks SectorID: %d Type: %d RemoteID: %s", task.task.SectorID, task.task.Type, task.task.RemoteID)
			sb.doTask(ctx, r, task)
		case <-ctx.Done():
			return
		case <-sb.stopping:
			return
		}


		r.lk.Lock()
		if r.remoteStatus >= WorkerCommit {
			r.remoteStatus = WorkerIdle
		} else if r.remoteStatus >= WorkerAddPiece {
			r.remoteStatus = r.remoteStatus + 1
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
