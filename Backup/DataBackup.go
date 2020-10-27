package main

import (
	"compress/gzip"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/urfave/cli/v2"
)

var (
	mBackupFolder, mHost, mPort, mUser, mPassword, mDatabase string
	mNumberBackup                                            int
)

func main() {
	app := cli.NewApp()

	app.Name = "Zabbix Backup tool"
	app.EnableBashCompletion = true

	app.Flags = []cli.Flag{
		&cli.PathFlag{
			Name:        "backup-root",
			Aliases:     []string{"r"},
			Usage:       "Backup root folder",
			Required:    true,
			TakesFile:   false,
			Destination: &mBackupFolder,
		},

		&cli.StringFlag{
			Name:        "host",
			Aliases:     []string{"H"},
			Usage:       "Mysql Host",
			Required:    false,
			Value:       "127.0.0.1",
			Destination: &mHost,
		},

		&cli.IntFlag{
			Name:        "number-backups",
			Aliases:     []string{"n"},
			Required:    false,
			Value:       0,
			Destination: &mNumberBackup,
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
			Value:       "mysql",
			Destination: &mDatabase,
		},
	}

	app.Action = func(context *cli.Context) error {
		//Create backup folder
		rootBackupDir := path.Join(mBackupFolder, time.Now().Format("02-01-2006-15-04"))
		dataBackupDir := path.Join(rootBackupDir, "data")
		err := os.MkdirAll(dataBackupDir, 0777)

		if err != nil {
			return err
		}

		// Do Tables Backup
		db, err := sql.Open("mysql", fmt.Sprintf(
			"%s:%s@tcp(%s:%s)/%s",
			mUser,
			mPassword,
			mHost,
			mPort,
			mDatabase,
		))

		if err != nil {
			return err
		}

		defer func() {
			_ = db.Close()
		}()

		tablesResult, err := db.Query("show tables;")

		if err != nil {
			return err
		}

		for tablesResult.Next() {
			table := ""
			err := tablesResult.Scan(&table)

			if err != nil {
				return nil
			}

			if table == "" {
				return errors.New("can't fetch table name")
			}

			err = doBackup(exec.Command(
				"mysqldump",
				"--port="+mPort,
				"--host="+mHost,
				"-u"+mUser,
				"-p"+mPassword,
				"--opt",
				"--no-create-info",
				"--extended-insert=FALSE",
				"--tables",
				table,
				mDatabase,
			), path.Join(dataBackupDir, fmt.Sprintf("%s.data.sql.gz", table)))

			if err != nil {
				fmt.Printf("Error: %s on %s table\n", err.Error(), table)
			}
		}

		//Do Schema backup
		err = doBackup(exec.Command(
			"mysqldump",
			"--port="+mPort,
			"--host="+mHost,
			"-u"+mUser,
			"-p"+mPassword,
			"--routines",
			"--opt",
			"--no-data",
			mDatabase,
		), path.Join(rootBackupDir, "zabbix.schema.sql.gz"))

		if err != nil {
			return err
		}

		if mNumberBackup > 0 {
			// Remove Old Backups
			backups, err := filepath.Glob(path.Join(mBackupFolder, "*"))

			if err != nil {
				return err
			}

			for i := 0; i < len(backups)-mNumberBackup; i++ {
				err = os.RemoveAll(backups[i])

				if err != nil {
					return err
				}
			}
		}

		return nil
	}

	err := app.Run(os.Args)

	if err != nil {
		log.Fatal(err)
	}
}

func doBackup(mysqldump *exec.Cmd, filePath string) error {
	//Start StdoutPipe
	stdout, err := mysqldump.StdoutPipe()

	if err != nil {
		return err
	}

	if err != nil {
		return err
	}

	defer func() {
		_ = stdout.Close()
	}()

	//start Backup
	if err := mysqldump.Start(); err != nil {
		log.Fatal(err)
	}

	//save backup
	backupFile, err := os.Create(filePath)

	if err != nil {
		return err
	}

	gz := gzip.NewWriter(backupFile)

	defer func() {
		_ = gz.Close()
		_ = backupFile.Sync()
		_ = backupFile.Close()
	}()

	_, err = io.Copy(gz, stdout)
	if err != nil {
		return err
	}

	return nil
}
