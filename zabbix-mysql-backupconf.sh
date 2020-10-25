#!/bin/bash
#
# zabbix-mysql-backupconf.sh
# v0.6 - 20170823 - added event_recovery to schema only
#                 - added purge old backups
# v0.5 - 20140203 - easier to upgrade (all and then exclude)
#
# Configuration Backup for Zabbix w/MySQL
#
# Author: Ricardo Santos (rsantos at gmail.com)
# http://zabbixzone.com
#
# modified by Brendon Baumgartner, 2014
#
# Contribution and Suggestions from::
# - Oleksiy Zagorskyi (zalex)
# - Petr Jendrejovsky
# - Jonathan Bayer
# - Jens Berthold
# - Brendon Baumgarter
#

MYSQL="mysql"
DBNAME="zabbix"
DBUSER="root"
DBPASS="123"
DBHOST="127.0.0.1"
DBPORT="3306"

BACKUPDIR=$1

# Delete old backups
if [ -d "$BACKUPDIR" ]; then
  echo Deleting old backups
  find $BACKUPDIR -type f -mtime +200 -exec rm {} \;
fi


if [ ! -x /usr/bin/mysqldump ]; then
	echo "mysqldump not found."
	echo "(with Debian, \"apt-get install mysql-client\" will help)"
	exit 1
fi

SCHEMA_ONLY="alerts auditlog auditlog_details events event_recovery history history_log history_str history_text history_uint trends_uint trends"

# If backing up all DBs on the server
echo "Getting Tables ...."
TABLES="`$MYSQL --host=$DBHOST --port=$DBPORT --user=$DBUSER --password=$DBPASS --batch --skip-column-names -e "show tables" $DBNAME`"

# remove excluded tables
for exclude in $SCHEMA_ONLY
do
	TABLES=`echo $TABLES | sed "s/\b$exclude\b//g"`
done

CONFTABLES=$TABLES

echo "Creating Backup Dir"
BACKUPDIR="${BACKUPDIR}/`date +%Y%m%d-%H%M`"
mkdir $BACKUPDIR
mkdir "$BACKUPDIR/data"

DUMPFILE_DATA="data.sql"
DUMPFILE_SCHEMA="${BACKUPDIR}/zabbix.schema.sql"

>"${DUMPFILE_SCHEMA}"

# CONFTABLES
for table in ${CONFTABLES[*]}; do
	echo "Backuping configuration table ${table}"
	mysqldump --routines --opt --single-transaction --skip-lock-tables --no-create-info --extended-insert=FALSE \
		-h ${DBHOST} --port=${DBPORT} -u ${DBUSER} -p${DBPASS} ${DBNAME} --tables ${table} >> "${BACKUPDIR}/data/${table}.${DUMPFILE_DATA}"
		gzip -f "${BACKUPDIR}/data/${table}.${DUMPFILE_DATA}" 2>&1
done

echo "Backuping schema"
mysqldump --routines --opt --single-transaction --skip-lock-tables --no-data	\
	-h ${DBHOST} -u ${DBUSER} -p${DBPASS} ${DBNAME} >>"${DUMPFILE_SCHEMA}"

gzip -f "${DUMPFILE_SCHEMA}"

echo
echo "Backup Completed"
