package main

import (
	"bytes"
	"fmt"
	"log"
	"math"
	"os"
	"strconv"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
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

	log.Printf("%+v\n", config)

	resp, err := svc.InitiateMultipartUpload(&glacier.InitiateMultipartUploadInput{
		AccountId:          aws.String("-"),
		VaultName:          &config.Vault,
		ArchiveDescription: &config.ArchiveDescription,
		PartSize:           &partSizeString,
	})

	if err != nil {
		log.Fatal("Failed to initiate multipart upload: ", err)
	}

	uploadID := *resp.UploadId

	log.Print("Upload id is ", uploadID)

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

	// calculate total number of parts the file will be chunked into
	totalPartsNum := uint64(math.Ceil(float64(fileSize) / float64(config.PartSize)))

	fmt.Printf("Splitting upload to %d pieces.\n", totalPartsNum)

	var wg sync.WaitGroup
	wg.Add(config.Concurrency)

	chunks := make(chan chunk, config.Concurrency*2)

	uploadChunks := func() {
		for c := range chunks {
			fmt.Printf("Chunk %s has size %x\n", c.rangeHeader, len(c.chunkBuffer))
			chunkReader := bytes.NewReader(c.chunkBuffer)

			h := glacier.ComputeHashes(chunkReader)

			chunkReader.Seek(0, 0)

			input := &glacier.UploadMultipartPartInput{
				AccountId: aws.String("-"),
				Body:      aws.ReadSeekCloser(chunkReader),
				Checksum:  aws.String(fmt.Sprintf("%x", h.TreeHash)),
				Range:     aws.String(c.rangeHeader),
				UploadId:  aws.String(uploadID),
				VaultName: aws.String(config.Vault),
			}

			result, err := svc.UploadMultipartPart(input)
			if err != nil {
				if aerr, ok := err.(awserr.Error); ok {
					switch aerr.Code() {
					case glacier.ErrCodeResourceNotFoundException:
						log.Println(glacier.ErrCodeResourceNotFoundException, aerr.Error())
					case glacier.ErrCodeInvalidParameterValueException:
						log.Println(glacier.ErrCodeInvalidParameterValueException, aerr.Error())
					case glacier.ErrCodeMissingParameterValueException:
						log.Println(glacier.ErrCodeMissingParameterValueException, aerr.Error())
					case glacier.ErrCodeRequestTimeoutException:
						log.Println(glacier.ErrCodeRequestTimeoutException, aerr.Error())
					case glacier.ErrCodeServiceUnavailableException:
						log.Println(glacier.ErrCodeServiceUnavailableException, aerr.Error())
					default:
						log.Println(aerr.Error())
					}
				} else {
					// Print the error, cast err to awserr.Error to get the Code and
					// Message from an error.
					log.Println(err.Error())
				}
				return
			}

			log.Println(result)
		}
		wg.Done()
	}

	for i := 0; i < config.Concurrency; i++ {
		go uploadChunks()
	}

	for i := uint64(0); i < totalPartsNum; i++ {
		chunkSize := int(math.Min(float64(config.PartSize), float64(fileSize-int64(i*config.PartSize))))
		partBuffer := make([]byte, chunkSize)

		file.Read(partBuffer)

		// set up chunk
		bytePosition := i * config.PartSize
		header := fmt.Sprintf("bytes %d-%d/%d", bytePosition, int(bytePosition)+chunkSize-1, fileSize)

		chunks <- chunk{rangeHeader: header, chunkBuffer: partBuffer}
	}

	// close channel and wait for all uploads
	close(chunks)
	wg.Wait()

	file.Seek(0, 0)
	h := glacier.ComputeHashes(file)

	fmt.Printf("Tree Hash: %x\n", h.TreeHash)

	input := &glacier.CompleteMultipartUploadInput{
		AccountId:   aws.String("-"),
		ArchiveSize: aws.String(strconv.FormatInt(fileSize, 10)),
		Checksum:    aws.String(fmt.Sprintf("%x", h.TreeHash)),
		UploadId:    aws.String(uploadID),
		VaultName:   aws.String(config.Vault),
	}

	result, err := svc.CompleteMultipartUpload(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case glacier.ErrCodeResourceNotFoundException:
				fmt.Println(glacier.ErrCodeResourceNotFoundException, aerr.Error())
			case glacier.ErrCodeInvalidParameterValueException:
				fmt.Println(glacier.ErrCodeInvalidParameterValueException, aerr.Error())
			case glacier.ErrCodeMissingParameterValueException:
				fmt.Println(glacier.ErrCodeMissingParameterValueException, aerr.Error())
			case glacier.ErrCodeServiceUnavailableException:
				fmt.Println(glacier.ErrCodeServiceUnavailableException, aerr.Error())
			default:
				fmt.Println(aerr.Error())
			}
		} else {
			// Print the error, cast err to awserr.Error to get the Code and
			// Message from an error.
			fmt.Println(err.Error())
		}
		return
	}

	fmt.Println(result)
}
