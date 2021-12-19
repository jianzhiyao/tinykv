package standalone_storage

import (
    "github.com/Connor1996/badger"
    "github.com/pingcap-incubator/tinykv/kv/storage"
    "github.com/pingcap-incubator/tinykv/kv/util/engine_util"
)

var _ storage.StorageReader = new(standaloneReader)

type standaloneReader struct {
    txn *badger.Txn
}

func newStandaloneReader(txn *badger.Txn) *standaloneReader {
    return &standaloneReader{
        txn: txn,
    }
}

func (s *standaloneReader) GetCF(cf string, key []byte) (b []byte, err error) {
    b, err = engine_util.GetCFFromTxn(s.txn, cf, key)
    switch err {
    case nil:
        // do nothing
    case badger.ErrKeyNotFound:
        err = nil
    default:
        return
    }

    return
}

func (s *standaloneReader) IterCF(cf string) engine_util.DBIterator {
    return engine_util.NewCFIterator(cf, s.txn)
}

func (s *standaloneReader) Close() {
    s.txn.Discard()
}
