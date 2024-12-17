#!/bin/sh
set -e

# Here we read from the environment variables $IPADDRESS and $PORT
exec /app/chord -a $IPADDRESS -p $PORT --ts 3000 --tff 1000 --tcp 3000 -r 4
