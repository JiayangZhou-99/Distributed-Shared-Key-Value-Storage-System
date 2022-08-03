package surfstore

import (
	context "context"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	grpc "google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

type RPCClient struct {
	MetaStoreAddrs []string
	BaseDir       string
	BlockSize     int
}

func (surfClient *RPCClient) GetBlock(blockHash string, blockStoreAddr string, block *Block) error {
	// connect to the server
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	b, err := c.GetBlock(ctx, &BlockHash{Hash: blockHash})
	if err != nil {
		conn.Close()
		return err
	}
	block.BlockData = b.BlockData
	block.BlockSize = b.BlockSize

	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) PutBlock(block *Block, blockStoreAddr string, succ *bool) error {
	// connect to the server

	conn, err := grpc.Dial(blockStoreAddr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)

	// perform the call
	defer conn.Close()
	succResponse, err := c.PutBlock(context.Background(), block)
	if err != nil {
		log.Panicln("put block error : ",err)
	}
	
	*succ = succResponse.Flag
	if err != nil {
		conn.Close()
		return err
	}

	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) HasBlocks(blockHashesIn []string, blockStoreAddr string, blockHashesOut *[]string) error {
	// connect to the server
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	blockHashResponse, err := c.HasBlocks(ctx,&BlockHashes{Hashes: blockHashesIn})

	*blockHashesOut = blockHashResponse.Hashes

	if err != nil {
		conn.Close()
		return err
	}
	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) GetFileInfoMap(serverFileInfoMap *map[string]*FileMetaData) error {
	println("fetching file map info")
	println(surfClient.MetaStoreAddrs)
	for _, metaStore := range surfClient.MetaStoreAddrs {

		conn, err := grpc.Dial(metaStore, grpc.WithInsecure())
		if err != nil {
			return err
		}
		c := NewRaftSurfstoreClient(conn)

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		f, err := c.GetFileInfoMap(ctx, &emptypb.Empty{})

		if err != nil {
			if strings.Contains(err.Error(), ERR_SERVER_CRASHED.Error()) {
				log.Println("error server crashed")
				continue
			}
			if strings.Contains(err.Error(), ERR_NOT_LEADER.Error()) {
				log.Println("error current server is not the leader")
				continue
			}
			conn.Close()
			return err
		}
		println("successfully fetched fileinfo")
		PrintMetaMap(f.FileInfoMap)
		*serverFileInfoMap = f.FileInfoMap
		return conn.Close()
	}
	return errors.New("Get file Info map cluster down")
}

func (surfClient *RPCClient) UpdateFile(fileMetaData *FileMetaData, latestVersion *int32) error {
	for idx, metaStore := range surfClient.MetaStoreAddrs {
		fmt.Println("Trying to update to metastore: ",idx, metaStore)
		conn, err := grpc.Dial(metaStore, grpc.WithInsecure())
		if err != nil {
			return err
		}
		c := NewRaftSurfstoreClient(conn)

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		v, err := c.UpdateFile(ctx, fileMetaData)

		if err != nil {
			if strings.Contains(err.Error(), ERR_SERVER_CRASHED.Error()) {
				continue
			}
			if strings.Contains(err.Error(), ERR_NOT_LEADER.Error()) {
				continue
			}
			conn.Close()
			return err
		}
		*latestVersion = v.Version
		return conn.Close()
	}
	return errors.New("update file cluster down")
}

func (surfClient *RPCClient) GetBlockStoreAddr(blockStoreAddr *string) error {
	for id, metaStore := range surfClient.MetaStoreAddrs {
		fmt.Println(id,metaStore)
		conn, err := grpc.Dial(metaStore, grpc.WithInsecure())

		if err != nil {
			return err
		}

		c := NewRaftSurfstoreClient(conn)

		// perform the call
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		addr, err := c.GetBlockStoreAddr(ctx, &emptypb.Empty{})
		if err != nil {
			fmt.Println(err)
			if strings.Contains(err.Error(), ERR_SERVER_CRASHED.Error()) {
				continue
			}
			if strings.Contains(err.Error(), ERR_NOT_LEADER.Error()) {
				continue
			}
			fmt.Println("getting block err",err)
			return err
		}
		*blockStoreAddr = addr.Addr
		return nil
	}
	return errors.New("get block store addr cluster down")
}

// This line guarantees all method for RPCClient are implemented
var _ ClientInterface = new(RPCClient)

// Create an Surfstore RPC client
func NewSurfstoreRPCClient(addrs []string, baseDir string, blockSize int) RPCClient {
	return RPCClient{
			MetaStoreAddrs: addrs,
			BaseDir:       baseDir,
			BlockSize:     blockSize,
	}
}
