package surfstore

import (
	"io"
	"io/ioutil"
	"log"
	"os"
	"crypto/sha256"
	"encoding/hex"

)

func SameHashList(hl1 []string, hl2 []string) bool {
	res := true
	if len(hl1) != len(hl2) {
		res = false
	} else {
		for i, _ := range hl1 {
			if hl1[i] != hl2[i] {
				res = false
				break
			}
		}
	}
	return res
}

func IsDeleted(hl *[]string) bool {
	return len(*hl) == 1 && (*hl)[0] == "0"
}

// Implement the logic for a client syncing with the server here.
func ClientSync(client RPCClient) {
	index_fm_map, err := LoadMetaFromMetaFile(client.BaseDir)
	if err != nil {
		log.Fatal(err)
	}
	files, err := ioutil.ReadDir(client.BaseDir)
	if err != nil {
        log.Fatal(err)
    }

	local_fm_map := make(map[string]*FileMetaData)
	client_block_map := make(map[string]*Block)

	for _, file := range files {
		if file.IsDir() || file.Name() == DEFAULT_META_FILENAME {
			continue
		}
        f, err := os.Open(ConcatPath(client.BaseDir, file.Name()))
		if err != nil {
			log.Fatal(err)
		}
		defer f.Close()
		single_meta := FileMetaData{}
		single_meta_hashlist := make([]string, 0)
		for {
			buf := make([]byte, client.BlockSize)
			bytesRead, err := f.Read(buf)
			if err != nil {
				if err == io.EOF {
					break
				}
				log.Fatal(err)
			}
			
			single_block := Block{}
			block_hash := sha256.Sum256(buf[:bytesRead])
			block_hash_str := hex.EncodeToString(block_hash[:])
			single_block.BlockData = buf[:bytesRead]
			single_block.BlockSize = int32(bytesRead)
			client_block_map[block_hash_str] = &single_block
			single_meta_hashlist = append(single_meta_hashlist, block_hash_str)
		}
		single_meta.Filename = file.Name()
		single_meta.Version = 1
		single_meta.BlockHashList = single_meta_hashlist

		local_fm_map[single_meta.Filename] = &single_meta
		f.Close()
    }

	user_deleted_file := make([]string, 0)
	user_new_file := make([]string, 0)
	// user_updated_file := make([]string, 0)

	for key, index_fm := range index_fm_map {
		if len(index_fm.BlockHashList) == 1 && index_fm.BlockHashList[0] == "0" {
			continue
		}
		if _, ok := local_fm_map[key]; !ok {
			// in index_fm but not in directory, deleted in directory
			user_deleted_file = append(user_deleted_file, key)
			index_fm.Version += 1
			index_fm.BlockHashList = []string{"0"}
		}
	}

	for key, local_fm := range local_fm_map {
		if index_fm, ok := index_fm_map[key]; !ok {
			// new in directory
			user_new_file = append(user_new_file, key)
			index_fm_map[key] = local_fm
		} else {
			// exist in both directory and index
			file_same := SameHashList(local_fm.BlockHashList, index_fm.BlockHashList)
			if !file_same {
				index_fm.Version += 1
				index_fm.BlockHashList = local_fm.BlockHashList
			}
		}
	}

	
	server_fm_map := make(map[string]*FileMetaData)
	err = client.GetFileInfoMap(&server_fm_map)
	if err != nil {
		log.Fatal(err)
	}

	for key, index_fm := range index_fm_map {
		server_fm, fm_exists_on_server := server_fm_map[key]
		if !fm_exists_on_server || index_fm.Version != server_fm.Version || !SameHashList(index_fm.BlockHashList, server_fm.BlockHashList) {
			// need update

			// if need to upload file, upload first before UpdateFile
			if !fm_exists_on_server || index_fm.Version == server_fm.Version + 1 {
				// put block to server
				if len(index_fm.BlockHashList) == 1 && index_fm.BlockHashList[0] == "0" {
					// deleted on local, do not put
				} else {
					// update succeed, put block to server
					for _, block_hash := range index_fm.BlockHashList {
						block := client_block_map[block_hash]
						succ := true
						block_addr := ""
						put_err := client.GetBlockStoreAddr(&block_addr)
						if put_err != nil {
							log.Fatal(put_err)
						}
						put_err = client.PutBlock(block, block_addr, &succ)
						if put_err != nil {
							log.Fatal(put_err)
						}
					}
				}
			}

			err = client.UpdateFile(index_fm, &index_fm.Version)
			if err != nil {
				// update failed, need to download
				if fm_exists_on_server {
					// only handle when file exists on server at check time.
					if len(server_fm.BlockHashList) == 1 && server_fm.BlockHashList[0] == "0" {
						// deleted on server
						w_err := os.Remove(ConcatPath(client.BaseDir, index_fm.Filename))
						if w_err != nil {
							log.Fatal(w_err)
						}
						index_fm.Version = server_fm.Version
						index_fm.BlockHashList = server_fm.BlockHashList
					} else {
						// get block and write to directory
						f_write, w_err := os.Create(ConcatPath(client.BaseDir, server_fm.Filename))
						if w_err != nil {
							log.Fatal(w_err)
						}
						defer f_write.Close()
						block_addr := ""
						w_err = client.GetBlockStoreAddr(&block_addr)
						if w_err != nil {
							log.Fatal(w_err)
						}
						for _, block_hash := range server_fm.BlockHashList {
							block_to_download := Block{}
							w_err = client.GetBlock(block_hash, block_addr, &block_to_download)
							if w_err != nil {
								log.Fatal(w_err)
							}
							_, w_err = f_write.Write(block_to_download.BlockData)
							if w_err != nil {
								log.Fatal(w_err)
							}
						}
						index_fm.Version = server_fm.Version
						index_fm.BlockHashList = server_fm.BlockHashList
					}
				}
			}

		}
	}

	for key, server_fm := range server_fm_map {
		_, fm_exists_on_index := index_fm_map[key]
		if !fm_exists_on_index {
			if IsDeleted(&server_fm.BlockHashList) {
				// no need to download if deleted
			} else {
				// file exist on server but not on index, need to download new files
				f_write, w_err := os.Create(ConcatPath(client.BaseDir, server_fm.Filename))
				if w_err != nil {
					log.Fatal(w_err)
				}
				defer f_write.Close()
				block_addr := ""
				w_err = client.GetBlockStoreAddr(&block_addr)
				if w_err != nil {
					log.Fatal(w_err)
				}
				for _, block_hash := range server_fm.BlockHashList {
					block_to_download := Block{}
					w_err = client.GetBlock(block_hash, block_addr, &block_to_download)
					if w_err != nil {
						log.Fatal(w_err)
					}
					_, w_err = f_write.Write(block_to_download.BlockData)
					if w_err != nil {
						log.Fatal(w_err)
					}
				}
			}
			new_file_meta := &FileMetaData{Filename: server_fm.Filename, Version: server_fm.Version, BlockHashList: server_fm.BlockHashList}
			index_fm_map[server_fm.Filename] = new_file_meta
		}
	}


	err = WriteMetaFile(index_fm_map, client.BaseDir)
	if err != nil {
		log.Fatal(err)
	}

	// for _, file_name := range user_new_file {
	// 	// put blocks to server
	// 	// update server file meta from local file meta, version = 0
	// 	// if version success
	// 	// update index file meta from local file meta
	// 	// else
	// 	// download from server
	// 	// update index file meta from server file meta
	// }

	// for _, file_name := range user_deleted_file {
	// 	// update server file meta from local file meta, version = index version + 1
	// 	// if version success
	// 	// update index file meta from local file meta
	// 	// else
	// 	// download from server
	// 	// update index file meta from server file meta
	// }

	// for _, file_name := range user_updated_file {
	// 	// 
	// }




}
