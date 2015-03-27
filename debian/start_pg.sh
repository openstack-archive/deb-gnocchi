#!/bin/sh
#
# Copyright (C) 2014 Thomas Goirand <zigo@debian.org>
#
# Runs pgsql server, then use that to run tests.
#

set -e
set -x

PG_MYTMPDIR=${1}

############################## 
### RUN THE PGSQL INSTANCE ###
##############################
MYUSER=`whoami`
# initdb refuses to run as root
if [ "${MYUSER}" = "root" ] ; then
	echo dropping root privs..
	exec /bin/su postgres -- "$0" "$@"
fi

BINDIR=`pg_config --bindir`

# depends on language-pack-en | language-pack-en
# because initdb acquires encoding from locale
export LC_ALL="C"
export LANGUAGE=C
PGSQL_PORT=9823
${BINDIR}/initdb -D ${PG_MYTMPDIR}

${BINDIR}/pg_ctl -w -D ${PG_MYTMPDIR} -o "-k ${PG_MYTMPDIR} -p ${PGSQL_PORT}" start > /dev/null
#${BINDIR}/postgres -D ${PG_MYTMPDIR} -h '' -k ${PG_MYTMPDIR} &
attempts=0
while ! [ -e ${PG_MYTMPDIR}/postmaster.pid ] ; do
	attempts=$((attempts+1))
	if [ "${attempts}" -gt 10 ] ; then
		echo "Exiting test: postgres pid file was not created after 30 seconds"
		exit 1
	fi
	sleep 3
	echo `date`: retrying..
done

# Set the env. var so that pgsql client doesn't use networking
# libpq uses this for all host params if not explicitly passed
export PGHOST=${PG_MYTMPDIR}

## Create a new test db
#createuser --superuser nailgun
#createdb -O nailgun nailgun
#export TEST_NAILGUN_DB=nailgun
