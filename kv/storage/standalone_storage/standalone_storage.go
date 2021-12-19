package standalone_storage

import (
    "github.com/Connor1996/badger"
    "github.com/pingcap-incubator/tinykv/kv/config"
    "github.com/pingcap-incubator/tinykv/kv/storage"
    "github.com/pingcap-incubator/tinykv/kv/util/engine_util"
    "github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
    "os"
    "path/filepath"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
    // Your Data Here (1).
    config *config.Config

    kvDB *badger.DB
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
    dbPath := conf.DBPath
    kvPath := filepath.Join(dbPath, "standalone")

    os.MkdirAll(kvPath, os.ModePerm)
    kvDB := engine_util.CreateDB(kvPath, false)

    return &StandAloneStorage{config: conf, kvDB: kvDB}
}

func (s *StandAloneStorage) Start() (err error) {
    return nil
}

func (s *StandAloneStorage) Stop() error {
    return s.kvDB.Close()
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
    txn := s.kvDB.NewTransaction(false)
    return newStandaloneReader(txn), nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) (err error) {
    err = s.kvDB.Update(func(txn *badger.Txn) (err error) {
        for _, m := range batch {
            switch action := m.Data.(type) {
            case storage.Put:
                err = engine_util.PutCFWithTxn(txn, action.Cf, action.Key, action.Value)
            case storage.Delete:
                err = engine_util.DeleteCFWithTxn(txn, action.Cf, action.Key)
            }
            if err != nil {
                return
            }
        }
        return
    })
    if err != nil {
        return
    }

    return nil
}
