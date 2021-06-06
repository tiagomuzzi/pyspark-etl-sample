# PySpark ETL Sample + Docker

This project includes an implementation of PySpark to process a dataset of events.

## Requirements

- python >= 3.6,<4.0
- poetry-core>=1.0.0
- pyspark = "^3.1.2"
- docker = (compatible with 20.10)

## Run

```sh
make all
```

This project includes a `Makefile`, which manages all project scripts:

- `lint` It lints the code using `pylint`
- `create-requirements` Creates `requirements.txt` from `pyproject` using `poetry`
- `build` Builds Image with Pyspark Environment. Includes `create-requirements`
- `run` It runs the build docker image and generates the outputs
- `all` Runs `build` and `run`

The script receives 1 input and 3 outputs:

| Type   | Generated | FileType | Value               | Desc                           |
| ------ | --------- | -------- | ------------------- | ------------------------------ |
| Input  | No        | JSON     | `generic_events`    | `datasets/generic_events.json` |
| Output | Yes       | Parquet  | `app_loaded_events` | `datasets/generic_events/`     |
| Output | Yes       | Parquet  | `registered_events` | `datasets/registered_events/`  |
| Output | Yes       | CSV      | `user_activity`     | `.outputs/user_activity/`      |
