package main

import (
	"bufio"
	"compress/gzip"
	"database/sql"
	"fmt"
	"log"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strings"

	_ "github.com/go-sql-driver/mysql"
	"github.com/urfave/cli/v2"
)

type onLineReaded func(line string)

type eachFile func(file *os.File)

var (
	mBackupFolder, mHost, mPort, mUser, mPassword, mDatabase string
	mWorkers                                                 int
)

func main() {
	app := cli.NewApp()

	app.Name = "Zabbix Restore tool"
	app.EnableBashCompletion = true

	app.Flags = []cli.Flag{
		&cli.PathFlag{
			Name:        "backup-folder",
			Aliases:     []string{"b"},
			Usage:       "Backup folder",
			Required:    true,
			TakesFile:   false,
			Destination: &mBackupFolder,
		},

		&cli.IntFlag{
			Name:        "workers",
			Aliases:     []string{"w"},
			Usage:       "Number of workers for data restore",
			Required:    false,
			Value:       runtime.NumCPU()*runtime.NumCPU(),
			Destination: &mWorkers,
		},

		&cli.StringFlag{
			Name:        "host",
			Aliases:     []string{"H"},
			Usage:       "Mysql Host",
			Required:    false,
			Value:       "127.0.0.1",
			Destination: &mHost,
		},

		&cli.StringFlag{
			Name:        "port",
			Aliases:     []string{"p"},
			Usage:       "Mysql Port",
			Required:    false,
			Value:       "3306",
			Destination: &mPort,
		},

		&cli.StringFlag{
			Name:        "user",
			Aliases:     []string{"u"},
			Usage:       "Mysql User",
			Required:    false,
			Value:       "root",
			Destination: &mUser,
		},

		&cli.StringFlag{
			Name:        "password",
			Aliases:     []string{"P"},
			Usage:       "Mysql Password",
			Required:    false,
			Value:       "123",
			Destination: &mPassword,
		},

		&cli.StringFlag{
			Name:        "database",
			Aliases:     []string{"d"},
			Usage:       "Mysql database",
			Required:    false,
			Value:       "zabbix",
			Destination: &mDatabase,
		},
	}

	app.Action = func(context *cli.Context) error {
		Files := make(chan *os.File)

		fmt.Printf("Starting %d Workers\n", mWorkers)
		for w := 1; w <= mWorkers; w++ {
			go startWorker(Files, createMysqlConnection(mHost, mPort, mUser, mPassword, mDatabase))
		}

		fmt.Println("Restoring...")

		db := createMysqlConnection(mHost, mPort, mUser, mPassword, mDatabase)

		defer func() {
			_ = db.Close()
		}()

		schemaFile, err := os.Open(path.Join(mBackupFolder, "zabbix.schema.sql.gz"))

		if err != nil {
			return err
		}

		schema, err := gzip.NewReader(schemaFile)

		if err != nil {
			return err
		}

		schemaScanner := bufio.NewScanner(schema)

		query := ""
		for schemaScanner.Scan() {
			s := schemaScanner.Text()

			if strings.HasPrefix(s, "-") || s == "" {
				continue
			}

			query += s

			if strings.HasSuffix(query, ";") {
				_, err := db.Exec(query)

				query = ""

				if err != nil {
					return err
				}
			}
		}

		if err := schemaScanner.Err(); err != nil {
			return err
		}

		forEachFile(path.Join(mBackupFolder, "data"), func(file *os.File) {
			Files <- file
		})

		return nil
	}

	err := app.Run(os.Args)

	if err != nil {
		log.Fatal(err)
	}
}

func createMysqlConnection(host, port, username, password, database string) *sql.DB {
	db, err := sql.Open("mysql", fmt.Sprintf(
		"%s:%s@tcp(%s:%s)/%s",
		username,
		password,
		host,
		port,
		database,
	))

	if err != nil {
		log.Fatal(err)
	}

	return db
}

func startWorker(files <-chan *os.File, db *sql.DB) {
	for f := range files {
		fmt.Printf("Start restoring %s\n", f.Name())
		readFile(f, func(line string) {
			_, err := db.Exec(line)
			if err != nil {
				log.Printf("Error: %s [%s]", err, line)
			}
		})
		fmt.Printf("Complete restoring %s\n", f.Name())
	}
}

func readFile(file *os.File, handle onLineReaded) {
	gz, err := gzip.NewReader(file)

	if err != nil {
		log.Fatal(err)
	}

	scanner := bufio.NewScanner(gz)

	for scanner.Scan() {
		s := scanner.Text()
		if s != "" {
			handle(s)
		}
	}

	if ScanErr := scanner.Err(); ScanErr != nil {
		log.Fatal(err)
	}
}

func forEachFile(path string, handle eachFile) {
	err := filepath.Walk(path, func(f string, info os.FileInfo, err error) error {
		if f == path {
			return nil
		}

		file, err := os.Open(f)

		handle(file)

		return err
	})

	if err != nil {
		log.Fatal(err)
	}
}
