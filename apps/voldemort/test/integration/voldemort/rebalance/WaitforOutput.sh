#!/bin/bash

set -x
remote_call=eval

if [ $# -gt 2 ]
then
  GREPSTR="("$1")|("$2")"
  LOGFILE=$3
  CMD=egrep
else
  GREPSTR=$1
  LOGFILE=$2
  CMD=grep
fi

echo entering wait loop....
$CMD "$GREPSTR" $LOGFILE > /dev/null 2>&1
while [ "$?" -ne "0" ]
do
  sleep 3
  $CMD "$GREPSTR" $LOGFILE > /dev/null 2>&1
done
echo exited wait loop!!!
