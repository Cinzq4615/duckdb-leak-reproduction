#!/bin/sh

set -e

# Path to the built JAR file
JAR_PATH=target/duckdb-leak-reproduction-1.0-SNAPSHOT.jar

# Make sure the JAR has been built
if [ ! -f "$JAR_PATH" ]; then
  echo "Code does not appear to have been built yet"
  exit 1
fi

# Fetch the duration out of the script arguments. Default to 1 minute
# if no duration is specified.
DURATION=PT1M
if [ -n "$1" ]; then
  DURATION=$1
fi

# Run the program in the background
java -jar "$JAR_PATH" "$DURATION" &
JAVA_PID=$!

# Make sure that we shut down the java process if this script exits
trap 'kill $JAVA_PID' INT

# Write a timestamp and the java process RSS out every second for as long
# as the process is running.
RSS=$(ps -o rss= -p $JAVA_PID)
while [ $? -eq 0 ]; do
  echo $RSS | ts '%Y-%m-%d %H:%M:%S,'
  sleep 1
  RSS=$(ps -o rss= -p $JAVA_PID)
done

# Clear the SIGINT trap, otherwise it'll complain about killing the
# (nonexistant) java process when this script ends
trap - INT
