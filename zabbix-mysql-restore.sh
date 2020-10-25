#!/bin/bash

DBNAME="zabbix2"
DBUSER="root"
DBPASS="123"
DBHOST="127.0.0.1"
DBPORT="3306"

BACKUPDIR=$1
WORKERS=$2


if [ ! -x /usr/bin/mysql ]; then
	echo "mysqldump not found."
	echo "(with Debian, \"apt-get install mysql-client\" will help)"
	exit 1
fi

gzip -d "${BACKUPDIR}/zabbix.schema.sql.gz"

mysql --host=$DBHOST --port=$DBPORT --user=$DBUSER --password=$DBPASS $DBNAME < "${BACKUPDIR}/zabbix.schema.sql"


./DataRestore "${BACKUPDIR}/data" $WORKERS $DBHOST $DBPORT $DBUSER $DBPORT $DBNAME