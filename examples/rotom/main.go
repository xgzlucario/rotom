package main

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"hash/crc32"
	"io/ioutil"
)

func main() {
	// 原始数据
	data := []byte("这是一些需要进行CRC校验和压缩的数据")

	// CRC 校验
	checksum := crc32.ChecksumIEEE(data)
	fmt.Printf("CRC Checksum: %d\n", checksum)

	// 压缩数据
	var buffer bytes.Buffer
	gz := gzip.NewWriter(&buffer)

	if _, err := gz.Write(data); err != nil {
		fmt.Println("压缩数据时出错: ", err)
		return
	}

	if err := gz.Close(); err != nil {
		fmt.Println("关闭gzip writer时出错: ", err)
		return
	}

	fmt.Println("Data after compression: ", buffer.Bytes())
}

// 对压缩后的数据进行解压缩
func Decompress(data []byte) ([]byte, error) {
	b := bytes.NewBuffer(data)
	var r *gzip.Reader
	var err error
	r, err = gzip.NewReader(b)
	if err != nil {
		return nil, fmt.Errorf("创建gzip reader时出错: %w", err)
	}
	defer r.Close()

	out, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("读取解压缩数据时出错: %w", err)
	}

	return out, nil
}
