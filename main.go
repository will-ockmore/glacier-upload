package main

import (
	"fmt"
	"log"
	"os"
	"strconv"

	"github.com/urfave/cli"
)

// Config to be passed to command
type Config struct {
	FilePath           string
	Vault              string
	ArchiveDescription string
	PartSize           uint64
	Concurrency        int
	Region             string
}

func main() {
	app := cli.NewApp()

	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:  "vault",
			Usage: "Vault on AWS Glacier",
		},
		cli.StringFlag{
			Name:  "archiveDescription",
			Usage: "Description for resulting archive on AWS Glacier",
		},
		cli.StringFlag{
			Name:  "partSize",
			Value: "4",
			Usage: "Size in MB to split multipart upload by." +
				" Must be between 1 and 4096, and must be a " +
				"multiple of two. Defaults to 4MB.",
		},
		cli.StringFlag{
			Name:  "concurrency",
			Value: "25",
			Usage: "How many worker threads to parallelize the multipart upload across. Defaults to 25.",
		},
		cli.StringFlag{
			Name:  "region",
			Usage: "Which AWS region to use.",
		},
	}

	app.Action = func(c *cli.Context) error {
		filePath := c.Args().Get(0)

		if filePath == "" {
			log.Fatal("Required argument filepath missing.")
		}

		if _, err := os.Stat(filePath); os.IsNotExist(err) {
			log.Fatal("Path does not exist.")
		}

		vault := c.String("vault")
		archiveDescription := c.String("archiveDescription")
		partSizeStr := c.String("partSize")
		concurrencyStr := c.String("concurrency")
		region := c.String("region")

		if vault == "" {
			log.Fatal("Required argument vault missing.")
		}

		partSize, err := strconv.ParseUint(partSizeStr, 10, 16)

		if err != nil {
			log.Fatal("partSize argument is not a valid integer.")
		}

		if !isPowerOfTwo(partSize) || partSize < 1 || partSize > 4096 {
			fmt.Printf("partSize given: %x\n", partSize)
			log.Fatal("partSize must be between 1 and 4096, and must be a multiple of two.")
		}

		concurrency, err := strconv.Atoi(concurrencyStr)

		if err != nil {
			log.Fatal("concurrency argument is not a valid integer.")
		}

		if concurrency < 1 {
			fmt.Printf("concurrency given: %x\n", concurrency)
			log.Fatal("concurrency must be a positive non-zero integer.")
		}

		uploadFileToGlacier(Config{
			FilePath:           filePath,
			Vault:              vault,
			ArchiveDescription: archiveDescription,
			PartSize:           partSize * (1 << 20), // part size in bytes
			Concurrency:        concurrency,
			Region:             region,
		})

		return nil
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}

// See https://stackoverflow.com/a/600306 for explanation
func isPowerOfTwo(x uint64) bool {
	return (x != 0) && ((x & (x - 1)) == 0)
}
