package surfstore

import (
	context "context"
	"sync"
)
var BlockStoreService = &BlockStore{}

type BlockStore struct {
	BlockMap map[string]*Block
	mtx sync.Mutex
	UnimplementedBlockStoreServer
}

func (bs *BlockStore) GetBlock(ctx context.Context, blockHash *BlockHash) (*Block, error) {
	bs.mtx.Lock()
	block := bs.BlockMap[blockHash.Hash]
	bs.mtx.Unlock()
	return block,nil
}

func (bs *BlockStore) PutBlock(ctx context.Context, block *Block) (*Success, error) {

	blockHashString := GetBlockHashString(block.BlockData)
	bs.mtx.Lock()
	bs.BlockMap[blockHashString] = block
	bs.mtx.Unlock()
	return &Success{Flag: true},nil
}

// Given a list of hashes “in”, returns a list containing the
// subset of in that are stored in the key-value store
func (bs *BlockStore) HasBlocks(ctx context.Context, blockHashesIn *BlockHashes) (*BlockHashes, error) {
	var hashes []string
	for _, hash := range blockHashesIn.Hashes {
		bs.mtx.Lock()
		if hashData, ok := bs.BlockMap[hash]; ok {
			hashes = append(hashes, string(hashData.BlockData))
		}
		bs.mtx.Unlock()
	}
	return &BlockHashes{Hashes: hashes}, nil
}

// This line guarantees all method for BlockStore are implemented
var _ BlockStoreInterface = new(BlockStore)

func NewBlockStore() *BlockStore {
	return &BlockStore{
		BlockMap: map[string]*Block{},
	}
}
