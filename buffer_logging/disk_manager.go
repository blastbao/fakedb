package buffer_logging

import (
	"github.com/blastbao/fakedb/util"
	"io"
	"os"
	"sync"
)

var diskLog = util.GetLog("disk")

type DiskManager struct {
	Lock   sync.Mutex
	Writer *os.File
}

const PageSize = 16 * 1024 // 16K

func NewDiskManager(dataFile string) (*DiskManager, error) {
	writer, err := os.OpenFile(dataFile, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}
	return &DiskManager{
		Writer: writer,
	}, nil
}

// 写入第 index 个页
func (disk *DiskManager) Write(index int32, page *Page) error {
	// 串行写
	disk.Lock.Lock()
	defer disk.Lock.Unlock()

	// 定位偏移
	_, err := disk.Writer.Seek(int64(index*PageSize), 0)
	if err != nil {
		return err
	}

	// 序列化
	data := page.Serialize()

	// 填充页
	if len(data) < PageSize {
		padding := make([]byte, PageSize-len(data))
		data = append(data, padding...)
	}

	// 写文件
	for i := uint32(0); i < page.Len(); {
		size, err := disk.Writer.Write(data[i:])
		if err != nil {
			return err
		}
		i += uint32(size)
	}

	return nil
}

// 读取第 index 个页
func (disk *DiskManager) Read(index int32) (page *Page, err error) {
	disk.Lock.Lock()
	defer disk.Lock.Unlock()

	// 定位到第 index 个页在文件中的偏移
	_, err = disk.Writer.Seek(int64(index*PageSize), 0)
	if err != nil {
		return
	}

	// 从偏移处读取 PageSize 个字节
	data := make([]byte, PageSize)
	i := 0
	for i = 0; i < PageSize; {
		size, err := disk.Writer.Read(data[i:])
		if err != nil && err != io.EOF {
			return nil, err
		}
		if err == io.EOF {
			err = nil
			break
		}
		i += size
	}

	// 反序列化成 Page
	page = &Page{}
	err1 := page.Deserialize(data[:i])
	if err1 != nil {
		// possible a broken packet.
		diskLog.InfoF("possible a broken packet on data. now ignore it.")
		return nil, nil
	}

	// 返回
	return
}
