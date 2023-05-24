# The BMS LakeAPI

<h1 align="center">
  <img src="https://raw.githubusercontent.com/bmsuisse/lakeapi/main/assets/LakeAPI.drawio.png">
  <br>
</h1>

[![tests](https://github.com/bmsuisse/lakeapi/actions/workflows/python-test.yml/badge.svg?branch=main)](https://github.com/bmsuisse/lakeapi/actions/workflows/python-test.yml)

A `FastAPI` plugin that allows you to expose your data lake as an API, allowing several output formats such as Parquet, Csv, Json, Excel, ...

The LakeAPI also includes a minimal security layer for convenience (Basic Auth), but you can also bring your own.

Unlike [roapi](https://github.com/roapi/roapi), we intentionally do not expose most SQL by default, but limit the possible queries with a config. This makes it easy for you to control what happens to your data. If you want the SQL endpoint, you can enable it. And because we built LakeAPI on the shoulders of giants like FastAPI. A lot of things like documentation and authentication are built in and we don't have to reinvent the wheel.

To run the application with the default config, just do it:

```python
app = FastAPI()
bmsdna.lakeapi.init_lakeapi(app)
```

To adjust the config, you can do like this:

```python
import dataclasses
import bmsdna.lakeapi

def_cfg = bmsdna.lakeapi.get_default_config() # Get default startup config
cfg = dataclasses.replace(def_cfg, enable_sql_endpoint=True, data_path="tests/data") # Use dataclasses.replace to set the properties you want
sti = bmsdna.lakeapi.init_lakeapi(app, cfg, "config_test.yml") # Enable it. The first parameter is the FastAPI instance, the 2nd one is the basic config and the third one the config of the tables
```

## Installation

[![PyPI version](https://badge.fury.io/py/bmsdna-lakeapi.svg)](https://pypi.org/project/bmsdna-lakeapi/)

Pypi Package `bmsdna-lakeapi` can be installed like any python package : `pip install bmsdna-lakeapi`

## Basic Idea

Based on a `YAML` configuration and the data source, LakeAPI will automatically generate GET and/or POST endpoints.
Calling the endpoint turns the query into an SQL statement that can be executed with the engine of your choice (duckdb, datafusion or polars).
The result is then seralised into the requested format (Json, CSV, Arrow etc).

## OpenAPI

Of course everything works with `OpenAPI` and `FastAPI`. This means you can add other FastAPI routes, you can use the /docs and /redoc endpoints.

So everything will be fully documented automatically, which is really cool. 🔥🔥

## Engine

`DuckDB` is the default query engine. `Polars` and `Datafusion` are also supported, but lack some features. The query engine can be specified at the route level and at the query level with the hidden parameter $engine="duckdb|datafusion|polars". If you want polars or datafusion, add the required extra.

At the moment, DuckDB seems to have an advantage and performs the best. Also features like full text search are only available with `DuckDB`.

## Default Security

By Default, Basic Authentication is enabled. To add a user, simply run `add_lakeapi_user YOURUSERNAME --yaml-file config.yml`. This will add the user to your config yaml (argon2 encrypted).
The generated Password is printed. If you do not want this logic, you can overwrite the username_retriver of the Default Config

## Standalone Mode

If you just want to run this thing, you can run it with a webserver:

Uvicorn: `uvicorn bmsdna.lakeapi.standalone:app --host 0.0.0.0 --port 8080`

Gunicorn: `gunicorn bmsdna.lakeapi.standalone:app --workers 4 --worker-class uvicorn.workers.UvicornWorker --bind 0.0.0.0:80`

Of course you need to adjust your http options as needed. Also, you need to `pip install` uvicorn/gunicorn

You can still use environment variables for configuration

## Environment Variables

- CONFIG_PATH: The path of the config file, defaults to `config.yml`. If you want to split the config, you can specify a folder, too
- DATA_PATH: The path of the data files, defaults to `data`. Paths in `config.yml` are relative to DATA_PATH
- ENABLE_SQL_ENDPOINT: Set this to 1 to enable the SQL Endpoint

## Config File

The application by default relies on a Config file beeing present at the root of your project that's call `config.yml`.

The config file looks something like this, see also [our test yaml](https://raw.githubusercontent.com/bmsuisse/lakeapi/dev/config_test.yml):

```yaml
tables:
  - name: fruits
    tag: test
    version: 1
    api_method:
      - get
      - post
    params:
      - name: cars
        operators:
          - "="
          - in
      - name: fruits
        operators:
          - "="
          - in
    datasource:
      uri: delta/fruits
      file_type: delta

  - name: fruits_partition
    tag: test
    version: 1
    api_method:
      - get
      - post
    params:
      - name: cars
        operators:
          - "="
          - in
      - name: fruits
        operators:
          - "="
          - in
      - name: pk
        combi:
          - fruits
          - cars
      - name: combi
        combi:
          - fruits
          - cars
    datasource:
      uri: delta/fruits_partition
      file_type: delta
      select:
        - name: A
        - name: fruits
        - name: B
        - name: cars

  - name: fake_delta
    tag: test
    version: 1
    allow_get_all_pages: true
    api_method:
      - get
      - post
    params:
      - name: name
        operators:
          - "="
      - name: name1
        operators:
          - "="
    datasource:
      uri: delta/fake
      file_type: delta

  - name: fake_delta_partition
    tag: test
    version: 1
    allow_get_all_pages: true
    api_method:
      - get
      - post
    params:
      - name: name
        operators:
          - "="
      - name: name1
        operators:
          - "="
    datasource:
      uri: delta/fake
      file_type: delta

  - name: "*" # We're lazy and want to expose all in that folder. Name MUST be * and nothing else
    tag: startest
    version: 1
    api_method:
      - post
    datasource:
      uri: startest/* # Uri MUST end with /*
      file_type: delta

  - name: fruits # But we want to overwrite this one
    tag: startest
    version: 1
    api_method:
      - get
    datasource:
      uri: startest/fruits
      file_type: delta
```

## Partitioning for awesome performance

To use partitions, you can either

- Partition by a column that you filter on. Obviously
- partition on a special column called `columnname_md5_prefix_2`, which means that you're partitioning on the first two characters of your hex-coded md5 hash.
  of the hexadecimal md5 hash. If you now filter by `columnname`, this will greatly reduce the number of files you search for. The number of characters used is up to you, we found two to be useful
- partition on a special column called `columnname_md5_mod_NRPARTITIONS`, where your partition value is `str(int(hashlib.md5(COLUMNNAME).hexdigest(), 16) % NRPARTITIONS)`. This might look a bit complicated, but it's not that hard :) You're just doing a modulo on your md5 hash, which allows you to
  which allows you to set the exact number of partitions. Filtering is still done correctly on `columnname`.

Why partition by MD5 hash? Imagine you have a product id where most id's start with a 1 and some newer ones start with a 2. Most of the data will be in the first partition. If you use an MD5 hash, the data will be spread evenly across the partitions.

With this hack you can get sub-second results on very large data. 🚀🚀

You need to use `deltalake` to use partitions, and you only need str partition columns for now.

[Z-ordering](https://docs.delta.io/latest/optimizations-oss.html#z-ordering-multi-dimensional-clustering) can also help a lot :).

## Even more features

- Built-in paging, you can use limit/offset to control what you get
- Full-text search using DuckDB's full-text search feature
- jsonify_complex parameter to convert structs/lists to json, the client cannot handle structs/lists
- Metadata endpoints to retrieve data types, string lengths and more
- Easily expose entire folders by using a "\*" wildcard in both the name and the datasource.uri config, see example in the config above
- Good test coverage

## Work in progress

Please note that this is a work in progress, changes will be made and things may break. Especially at this early stage.

## Contribution

Feel free to contribute, report bugs or request enhancements.
