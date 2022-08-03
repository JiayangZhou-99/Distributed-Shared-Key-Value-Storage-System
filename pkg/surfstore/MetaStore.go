package surfstore

import (
	context "context"
	"sync"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

var MetaStoreService  = &MetaStore{}

type MetaStore struct {
	FileMetaMap    map[string]*FileMetaData
	BlockStoreAddr string
	mtx   sync.Mutex
	UnimplementedMetaStoreServer

}

func (m *MetaStore) GetFileInfoMap(ctx context.Context, _ *emptypb.Empty) (*FileInfoMap, error) {
	return &FileInfoMap{FileInfoMap: m.FileMetaMap},nil
}

func (m *MetaStore) UpdateFile(ctx context.Context, fileMetaData *FileMetaData) (*Version, error) {
	
	filename := fileMetaData.Filename
	version  := fileMetaData.Version
	print("upload to metastore: ",filename,version);
	m.mtx.Lock()
	if _, ok := m.FileMetaMap[filename]; ok {
		// if exist on server
		if version > m.FileMetaMap[filename].Version {
			//if local file is newer than the file on the server
			m.FileMetaMap[filename] = fileMetaData
		}else if version==-1{
			m.FileMetaMap[filename] = fileMetaData
		}else {
			//if server file is newer than the file on the local file
			fileMetaData = m.FileMetaMap[filename]
			version = m.FileMetaMap[filename].Version
		}
	} else {
		println(" not on server")
		// if not on server
		m.FileMetaMap[filename] = fileMetaData
		println("successfully uploaded")
	}
	m.mtx.Unlock()
	PrintMetaMap(m.FileMetaMap)
	return &Version{Version: version}, nil
}

func (m *MetaStore) GetBlockStoreAddr(ctx context.Context, _ *emptypb.Empty) (*BlockStoreAddr, error) {
	return &BlockStoreAddr{Addr: m.BlockStoreAddr},nil
}

// This line guarantees all method for MetaStore are implemented
var _ MetaStoreInterface = new(MetaStore)

func NewMetaStore(blockStoreAddr string) *MetaStore {
	return &MetaStore{
		FileMetaMap:    map[string]*FileMetaData{},
		BlockStoreAddr: blockStoreAddr,
	}
}
