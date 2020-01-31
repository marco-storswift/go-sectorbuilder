package sectorbuilder

import (
	"github.com/filecoin-project/go-address"
	"github.com/ipfs/go-datastore"
)

func TempSectorbuilderDir(paths string, sectorSize uint64, ds datastore.Batching) (*SectorBuilder, error) {
	addr, err := address.NewFromString("t0123")
	if err != nil {
		return nil, err
	}

	sb, err := New(&Config{
		SectorSize: sectorSize,

		Dir: paths,

		WorkerThreads: 2,
		Miner:         addr,
	}, ds)
	if err != nil {
		return nil, err
	}

	return sb, nil
}

//func SimplePath(dir string) []fs.PathConfig {
//	return []fs.PathConfig{{
//		Path:   dir,
//		Cache:  true,
//		Weight: 1,
//	}}
//}

func SimplePath(dir string) string {
     return dir
}