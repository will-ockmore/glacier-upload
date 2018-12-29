package main

import (
	"fmt"
	"log"
	"math"
	"os"
	"strconv"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/glacier"
)

type chunk struct {
	chunkBuffer []byte
	rangeHeader string
}

func uploadFileToGlacier(config Config) {
	svc := glacier.New(session.New(&aws.Config{Region: aws.String(config.Region)}))

	partSizeString := strconv.FormatUint(config.PartSize, 10)

	resp, err := svc.InitiateMultipartUpload(&glacier.InitiateMultipartUploadInput{
		AccountId:          aws.String("-"),
		VaultName:          &config.Vault,
		ArchiveDescription: &config.ArchiveDescription,
		PartSize:           &partSizeString,
	})

	if err != nil {
		log.Fatal("Failed to initiate multipart upload: ", err)
	}

	log.Print("Upload id is ", *resp.UploadId)

	file, err := os.Open(config.FilePath)

	if err != nil {
		log.Fatal(err)
	}

	defer file.Close()

	fileInfo, err := file.Stat()

	if err != nil {
		log.Fatal(err)
	}

	fileSize := fileInfo.Size()

	// chunk size in bytes
	chunkSize := config.PartSize * (1 << 20)

	// calculate total number of parts the file will be chunked into
	totalPartsNum := uint64(math.Ceil(float64(fileSize) / float64(chunkSize)))

	fmt.Printf("Splitting upload to %d pieces.\n", totalPartsNum)

	var wg sync.WaitGroup
	wg.Add(config.Concurrency)

	chunks := make(chan chunk)

	uploadChunks := func() {
		for c := range chunks {
			fmt.Printf("Chunk %s has size %x\n", c.rangeHeader, len(c.chunkBuffer))
			// TODO: upload chunk with hash to glacier
		}
		wg.Done()
	}

	for i := 0; i < config.Concurrency; i++ {
		go uploadChunks()
	}

	for i := uint64(0); i < totalPartsNum; i++ {
		partSize := int(math.Min(float64(chunkSize), float64(fileSize-int64(i*chunkSize))))
		partBuffer := make([]byte, partSize)

		file.Read(partBuffer)

		// set up chunk
		bytePosition := i * chunkSize
		header := fmt.Sprintf("bytes %d-%d/%d", bytePosition, int(bytePosition)+partSize-1, fileSize)

		chunks <- chunk{rangeHeader: header, chunkBuffer: partBuffer}
	}

	// close channel and wait for all uploads
	close(chunks)
	wg.Wait()

	file.Seek(0, 0)
	h := glacier.ComputeHashes(file)

	fmt.Printf("Tree Hash: %x\n", h.TreeHash)
}
