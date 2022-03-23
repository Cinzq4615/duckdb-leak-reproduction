# DuckDB Memory Leak Reproduction

We believe we've discovered a memory leak somewhere in DuckDB during the running of one of our services. This long-lived
process sees continually growing RSS (resident set size) memory until we disable (via feature flag) all DuckDB activity.

We've written the code in this repository to approximate what our service is doing. In short it performs the following:

1. Creates an in-memory DuckDB database
2. Creates a background thread pool
3. Submits table creation + data loading tasks at a fixed rate to the thread pool
4. Drops tables as necessary to maintain 100 tables at any given time
