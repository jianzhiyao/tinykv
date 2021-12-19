package server

import (
    "context"
    "github.com/Connor1996/badger"
    "github.com/pingcap-incubator/tinykv/kv/storage"
    "github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (resp *kvrpcpb.RawGetResponse, err error) {
    resp = &kvrpcpb.RawGetResponse{}
    defer func() {
        if err != nil {
            resp.Error = err.Error()
        }
    }()

    reader, err := server.storage.Reader(req.GetContext())
    if err != nil {
        return
    }
    defer reader.Close()

    value, err := reader.GetCF(req.GetCf(), req.GetKey())
    switch err {
    case nil:
        // do nothing
    case badger.ErrKeyNotFound:
        err = nil
    default:
        return
    }
    resp.Value, resp.NotFound = value, value == nil

    return
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (resp *kvrpcpb.RawPutResponse, err error) {
    resp = &kvrpcpb.RawPutResponse{}
    defer func() {
        if err != nil {
            resp.Error = err.Error()
        }
    }()

    err = server.storage.Write(req.GetContext(), []storage.Modify{
        {
            Data: storage.Put{
                Key:   req.GetKey(),
                Value: req.GetValue(),
                Cf:    req.GetCf(),
            },
        },
    })
    if err != nil {
        return
    }

    return
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (resp *kvrpcpb.RawDeleteResponse, err error) {
    resp = &kvrpcpb.RawDeleteResponse{}
    defer func() {
        if err != nil {
            resp.Error = err.Error()
        }
    }()

    err = server.storage.Write(req.GetContext(), []storage.Modify{
        {
            Data: storage.Delete{
                Key: req.GetKey(),
                Cf:  req.GetCf(),
            },
        },
    })

    if err != nil {
        return
    }

    return
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (resp *kvrpcpb.RawScanResponse, err error) {
    resp = &kvrpcpb.RawScanResponse{}
    defer func() {
        if err != nil {
            resp.Error = err.Error()
        }
    }()

    reader, err := server.storage.Reader(req.GetContext())
    if err != nil {
        return nil, err
    }
    defer reader.Close()

    iter := reader.IterCF(req.Cf)
    defer iter.Close()

    iter.Seek(req.GetStartKey())
    limit := req.GetLimit()
    for i := uint32(0); i < limit; i++ {
        if !iter.Valid() {
            break
        }
        item := iter.Item()
        value, _ := item.Value()
        resp.Kvs = append(resp.Kvs, &kvrpcpb.KvPair{
            Error: nil,
            Key:   item.Key(),
            Value: value,
        })
        iter.Next()
    }

    return
}
