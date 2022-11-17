package surfstore

import (
	context "context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
)

type BlockStore struct {
	BlockMap map[string]*Block
	UnimplementedBlockStoreServer
}

func (bs *BlockStore) GetBlock(ctx context.Context, blockHash *BlockHash) (*Block, error) {
	get_block, ok := bs.BlockMap[blockHash.Hash]
	if ok {
		return &Block{
			BlockData: get_block.BlockData,
			BlockSize: get_block.BlockSize,
		}, nil
	}
	return &Block{}, fmt.Errorf("hash not found")
}

func (bs *BlockStore) PutBlock(ctx context.Context, block *Block) (*Success, error) {
	block_hash := sha256.Sum256(block.BlockData)
	block_hash_string := hex.EncodeToString(block_hash[:])
	if _, ok := bs.BlockMap[block_hash_string]; !ok {
		bs.BlockMap[block_hash_string] = block
		return & Success{
			Flag: true,
		}, nil
	} else {
		return & Success{
			Flag: false,
		}, nil
	}
}

// Given a list of hashes “in”, returns a list containing the
// subset of in that are stored in the key-value store
func (bs *BlockStore) HasBlocks(ctx context.Context, blockHashesIn *BlockHashes) (*BlockHashes, error) {
	block_hashes := &BlockHashes{}
	for _, hash := range blockHashesIn.Hashes {
		if _, ok := bs.BlockMap[hash]; ok {
			block_hashes.Hashes = append(block_hashes.Hashes, hash)
		}
	}
	return block_hashes, nil
}

// This line guarantees all method for BlockStore are implemented
var _ BlockStoreInterface = new(BlockStore)

func NewBlockStore() *BlockStore {
	return &BlockStore{
		BlockMap: map[string]*Block{},
	}
}
