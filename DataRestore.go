package main

import (
	"bufio"
	"compress/gzip"
	"database/sql"
	"fmt"
	"github.com/cheggaaa/pb/v3"
	_ "github.com/go-sql-driver/mysql"
	"github.com/urfave/cli/v2"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strings"
)

type OnLineReaded func(line string)

type EachFile func(file *os.File)

var (
	BackupFolder, Host, Port, User, Password, Database string
	Workers                                            int
	HideProgress                                       bool
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
			Destination: &BackupFolder,
		},

		&cli.IntFlag{
			Name:        "workers",
			Aliases:     []string{"w"},
			Usage:       "Number of workers for data restore",
			Required:    false,
			Value:       runtime.NumCPU(),
			Destination: &Workers,
		},

		&cli.StringFlag{
			Name:        "host",
			Aliases:     []string{"H"},
			Usage:       "Mysql Host",
			Required:    false,
			Value:       "127.0.0.1",
			Destination: &Host,
		},

		&cli.StringFlag{
			Name:        "port",
			Aliases:     []string{"p"},
			Usage:       "Mysql Port",
			Required:    false,
			Value:       "3306",
			Destination: &Port,
		},

		&cli.StringFlag{
			Name:        "user",
			Aliases:     []string{"u"},
			Usage:       "Mysql User",
			Required:    false,
			Value:       "root",
			Destination: &User,
		},

		&cli.StringFlag{
			Name:        "password",
			Aliases:     []string{"P"},
			Usage:       "Mysql Password",
			Required:    false,
			Value:       "123",
			Destination: &Password,
		},

		&cli.StringFlag{
			Name:        "database",
			Aliases:     []string{"d"},
			Usage:       "Mysql database",
			Required:    false,
			Value:       "zabbix",
			Destination: &Database,
		},

		&cli.BoolFlag{
			Name:        "hide-progress",
			Aliases:     []string{"hp"},
			Usage:       "Hide Progress bar",
			Value:       false,
			HasBeenSet:  false,
			Destination: &HideProgress,
		},
	}

	app.Action = func(context *cli.Context) error {
		Files := make(chan *os.File)

		var progress *pb.ProgressBar

		if !HideProgress {
			fs, _ := ioutil.ReadDir(BackupFolder + "/data")
			progress = pb.Default.New(len(fs) + 1)
		}

		fmt.Printf("Starting %d Workers\n", Workers)
		for w := 1; w <= Workers; w++ {
			go StartWorker(Files, CreateMysqlConnection(Host, Port, User, Password, Database), progress)
		}

		fmt.Println("Restoring...")

		db := CreateMysqlConnection(Host, Port, User, Password, Database)

		defer func() {
			_ = db.Close()
		}()

		if !HideProgress {
			progress.Start()
		}

		schemaFile, err := os.Open(BackupFolder + "/zabbix.schema.sql.gz")

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

		if !HideProgress {
			progress.Increment()
		}

		ForEachFile(BackupFolder+"/data", func(file *os.File) {
			Files <- file
		})

		return nil
	}

	err := app.Run(os.Args)

	if err != nil {
		log.Fatal(err)
	}
}

func CreateMysqlConnection(host, port, username, password, database string) *sql.DB {
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

func StartWorker(files <-chan *os.File, db *sql.DB, progress *pb.ProgressBar) {
	if !HideProgress {
		for f := range files {
			progress.Increment()
			ReadFile(f, func(line string) {
				_, err := db.Exec(line)
				if err != nil {
					log.Printf("Error: %s [%s]", err, line)
				}
			})
		}
	} else {
		for f := range files {
			ReadFile(f, func(line string) {
				_, err := db.Exec(line)
				if err != nil {
					log.Printf("Error: %s [%s]", err, line)
				}
			})
		}
	}
}

func ReadFile(file *os.File, handle OnLineReaded) {
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

func ForEachFile(path string, handle EachFile) {
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
