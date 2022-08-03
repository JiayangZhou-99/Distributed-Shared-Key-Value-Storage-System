package surfstore

import (
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"os"
	"reflect"
)

// Implement the logic for a client syncing with the server here.


func ClientSync(client RPCClient) {

	
	serverFileMetaInfoMap := make(map[string]*FileMetaData)
	localFileMetaInfoMap := make(map[string]*FileMetaData)

	localFileMetaInfoMap,err := LoadMetaFromMetaFile(client.BaseDir)
	err = client.GetFileInfoMap(&serverFileMetaInfoMap)
	if err != nil {
		log.Fatal(err)
	}

	files, _ := ioutil.ReadDir(client.BaseDir)
	var serverFileMetaInfo *FileMetaData
	var localFileMetaInfo  *FileMetaData

	//read in local file to update local meta first
	for _, f := range files {
		if(f.Name() == DEFAULT_META_FILENAME){
			continue
		}
		if _, ok := localFileMetaInfoMap[f.Name()]; !ok {
			fileHashgBlocks := client.readInFile(f.Name())
			localFileMetaInfoMap[f.Name()] = &FileMetaData{Filename: f.Name(),Version: 1,BlockHashList: fileHashgBlocks}
		}
	}
	// err = WriteMetaFile(localFileMetaInfoMap,client.BaseDir)
	// if err != nil {
	// 	log.Fatal(err)
	// }
	/////////
	PrintMetaMap(localFileMetaInfoMap)
	PrintMetaMap(serverFileMetaInfoMap)
	// scan local files and upload or update to the server
    for _, f := range files {
		log.Println("file Name : ",f.Name())
		if(f.Name() == DEFAULT_META_FILENAME){
			continue
		}

		// if do not exis on the server,push it to the server
		if _, ok := serverFileMetaInfoMap[f.Name()]; !ok {
			fmt.Println("pushing",f.Name()," to server")
			client.readInFile(f.Name())
			
			if err != nil {
				fmt.Println("adding file to localFileMap fail",err)
				log.Fatal()
			}

			fileHashgBlocks := client.readInFile(f.Name())
			localFileMetaInfoMap[f.Name()] = &FileMetaData{Filename: f.Name(),Version: 1,BlockHashList: fileHashgBlocks}
			client.pushToServer(localFileMetaInfoMap[f.Name()])
			println("finished push to server")
			// err = client.GetFileInfoMap(&serverFileMetaInfoMap)
			// PrintMetaMap(serverFileMetaInfoMap)
			continue
		}else{
		//it exists on the server
		
			err = client.GetFileInfoMap(&serverFileMetaInfoMap)
			localFileMetaInfo  = localFileMetaInfoMap[f.Name()]
			serverFileMetaInfo = serverFileMetaInfoMap[f.Name()]

			//scan files check whether bee	n modified and update the version
			if( reflect.DeepEqual(localFileMetaInfo,serverFileMetaInfo)){
				continue
			}else{
				if(localFileMetaInfo.Version < serverFileMetaInfo.Version){
					fmt.Println("colision happpened, You have to update your local file first and then do the update")
					// log.Fatal()
				}
			}

			if(serverFileMetaInfo.Version == -1){
				fmt.Println(" delete started ")
				localFileMetaInfo.Version = -1
				err = os.Remove(client.BaseDir+"/"+f.Name())
				if err != nil {
					log.Println("delete file fail",err)
					log.Fatal()
				}
				delete(localFileMetaInfoMap,f.Name())

			}else if(localFileMetaInfo.Version > serverFileMetaInfo.Version){
				serverFileMetaInfo.Version 		 = localFileMetaInfo.Version
				serverFileMetaInfo.BlockHashList = localFileMetaInfo.BlockHashList
				//update the remote file in MetaStore and Blockstore
				fileHashgBlocks := client.readInFile(f.Name())
				localFileMetaInfoMap[f.Name()] = &FileMetaData{Filename: f.Name(),Version: localFileMetaInfo.Version,BlockHashList: fileHashgBlocks}
				client.pushToServer(serverFileMetaInfo)
		
			}else if(localFileMetaInfo.Version < serverFileMetaInfo.Version){
				localFileMetaInfo.Version 		 = serverFileMetaInfo.Version
				localFileMetaInfo.BlockHashList  = serverFileMetaInfo.BlockHashList
				//write into the local file
				client.pullToLocal(localFileMetaInfo)
			}else{
				if(serverFileMetaInfo.Version<0){
					localFileMetaInfo.Version = 1
				}else{
					localFileMetaInfo.Version = serverFileMetaInfo.Version + 1
				}
				localFileMetaInfo.BlockHashList  = serverFileMetaInfo.BlockHashList
				//write into the local file
				client.pushToServer(localFileMetaInfo)
			}
		}
    }

	println("finished local to server update")
	// NewServerFileMetaInfoMap := make(map[string]*FileMetaData)
	err = client.GetFileInfoMap(&serverFileMetaInfoMap)
	if err != nil {
		log.Fatal("fetching fileInfoError",err)
	}
	// PrintMetaMap(localFileMetaInfoMap)
	PrintMetaMap(serverFileMetaInfoMap)

	//if some of the files gets deleted, the file record will be in the localFileMetaInfo, but not under the directory
	for localFileName,_ := range localFileMetaInfoMap {
		
		_, err := os.Stat(client.BaseDir + "/" + localFileName)
		if !os.IsNotExist(err) {
			continue
		}
		fmt.Println("gets deleted",localFileName)
		localFileMetaInfoMap[localFileName].Version = -1
		serverFileMetaInfoMap[localFileName].Version = -1
	}

	//if have new file on the server, add to local
	for k, v := range serverFileMetaInfoMap {
		if _, ok := localFileMetaInfoMap[k]; !ok {
			// if do not exis locally
			println("pull server file ",k,"to local")
			if(v.Version ==-1){
				continue
			}
			localFileMetaInfoMap[k] = v
			client.pullToLocal(localFileMetaInfoMap[k])
		}else{
			if(localFileMetaInfoMap[k].Version == -1){
				var latestV int32 
				client.UpdateFile(localFileMetaInfoMap[k],&latestV)
				continue
			}
			client.pullToLocal(localFileMetaInfoMap[k])
		}
	}
	
	err = WriteMetaFile(localFileMetaInfoMap,client.BaseDir)
	if err != nil {
		log.Fatal(err)
	}

}

func (client RPCClient) readInFile(fileName string) []string{
	file,err := ioutil.ReadFile(client.BaseDir + "/" + fileName)
	if err != nil {
		fmt.Println("readInFileError",err)
	}

	var numBlocks int = int(math.Ceil(float64(len(file)) / float64(client.BlockSize)))
	fileToRead, err := os.Open(client.BaseDir + "/" + fileName)

	if err != nil {
		log.Println("Error reading file in basedir: ", err)
	}
	
	var blockAddr string
	err = client.GetBlockStoreAddr(&blockAddr)
	if err != nil {
		log.Fatal(err)
	}

	var hashList []string
	for i := 0; i < numBlocks; i++ {
		byteSlice := make([]byte, client.BlockSize)
		len, err := fileToRead.Read(byteSlice)
		if err != nil{
			log.Println("Error reading bytes from file in basedir: ", err)
		}
		byteSlice = byteSlice[:len]
		hash := GetBlockHashString(byteSlice)
		hashList = append(hashList, hash)
		
		var success  bool
		fmt.Println("block storage address: ",blockAddr)
		err = client.PutBlock(&Block{BlockData: byteSlice,BlockSize: int32(len)},blockAddr,&success)
		if err != nil {
			log.Panicln("PutBlock err:  ",err)
		}
	}

	return hashList
}

func (client RPCClient) writeToDesk(filePath string,serverFileMetaInfo *FileMetaData) error{

	_, err := os.Stat(filePath)
	if !os.IsNotExist(err) {
		os.Remove(filePath)
	}

	file, err := os.Create(filePath)
	if err != nil {
		log.Println(err)
		return nil
	}
	defer file.Close()
	
	blockHashList := serverFileMetaInfo.BlockHashList

	var blockStoreAddr string
	err = client.GetBlockStoreAddr(&blockStoreAddr)
	if err != nil {
		fmt.Println("writeToDesk err",err)
		return err
	}

	for _, blockHash := range blockHashList {
		var block Block
		err := client.GetBlock(blockHash, blockStoreAddr, &block)
		if err != nil {
			log.Println(err)
		}
		// fmt.Println(block.BlockSize,block.BlockData)
		_, err = file.Write(block.BlockData)
		if err != nil {
			log.Println(err)
		}
	}

	return nil
}

func (client RPCClient) pullToLocal(serverFileMetaInfo *FileMetaData) error {
	//First: update the DEFAULT_META_FILENAME(index.txt) then update the correponding file
	err := client.writeToDesk(client.BaseDir+"/"+serverFileMetaInfo.Filename,serverFileMetaInfo)
	if err != nil {
		log.Panicln("pullToLocal error",err)
	}
	return nil;
}


func (client RPCClient) pushToServer(localFileMetaInfo *FileMetaData) error {
	//First: update the Metastore then update the Blockstore
	client.UpdateFile(localFileMetaInfo,&localFileMetaInfo.Version)
	return nil;
}
