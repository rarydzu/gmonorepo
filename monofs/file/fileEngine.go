package file

// FsFileEngine managing pool of files assign proper client to file handle and managing space on disk
// it also managing locking and unlocking files
type FsFileEngine struct {
}

// NewFsFileEngine creates new FsFileEngine object
func NewFsFileEngine() *FsFileEngine {
	return &FsFileEngine{}
}

// TODO use custo, KV db for every file and on close index should keep trace of current blocks in 4k pages ex: 0 -> 2.4 - block 0 == file 2 block 4 , sync asynchornous send data fro db compressed with snappy to proxy / S3 etc
// reading object from proxy split it to 4k blocks put them to custom db ( index should nbe fast as it is 1:1 mapping)
