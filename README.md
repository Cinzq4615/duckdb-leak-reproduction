# DuckDB Memory Leak Reproduction

We believe we've discovered a memory leak somewhere in DuckDB during the running of one of our services. This long-lived
process sees continually growing RSS (resident set size) memory until we disable (via feature flag) all DuckDB activity.

We've written the code in this repository to approximate what our service is doing. In short it performs the following:

1. Creates an in-memory DuckDB database
2. Creates a background thread pool
3. Submits table creation + data loading tasks at a fixed rate to the thread pool
4. Drops tables as necessary to maintain 100 tables at any given time

## Running the Program

1. Build the program: `./mvnw clean package`
2. Run the program: `./run.sh [duration]` (e.g. `./run.sh PT1H`).

The duration argument is optional. If given it should conform to the format specified by
[java.time.Duration#parse(CharSequence)](https://docs.oracle.com/javase/8/docs/api/java/time/Duration.html#parse-java.lang.CharSequence-)
.

The script will output the Java process RSS once per second with timestamps, which is helpful for charting RSS use over
time:

```
% ./run.sh PT1H
15:24:02 2032
15:24:03 160960
15:24:04 196288
15:24:05 215168
15:24:06 224592
15:24:07 253424
15:24:09 295056
15:24:10 337712
15:24:11 379520
...
```
