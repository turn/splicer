#!/bin/bash

# runs the java class provided as parameter to this shell script.

PORT=$SPLICER_PORT

[[ -z "${PORT}" ]] && echo -e 'Port number is not set in TSDB_PORT. Using default 4245' && PORT='4245'

BASE=`pwd`

echo "Working directory" ${BASE}, 'PORT=' $PORT

java -server -classpath /usr/share/tsdb-splicer/resources/:${BASE}/tsdb-splicer-all-0.1.1.jar com.turn.splicer.SplicerMain --port ${PORT} --config=${BASE}/resources/splicer.conf
