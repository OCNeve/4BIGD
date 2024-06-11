# Startup
_this project depends on docker, make sure the engine is running before starting the project._
## Setup
First of all, you must pull remote images and build local ones, for this, position yourself at the project root and run the following command.
```bash
docker compose build
```
## Running the project
To startup the whole infrastructure and run the pipeline, run the following command.
```bash
docker compose up -d
```

## Reading the pipeline logs
So as to see what the pipeline is up to, and read the results of the data validation checks, you can run the following command.
```bash
docker logs --follow pyspark
```