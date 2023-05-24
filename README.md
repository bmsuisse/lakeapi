# The BMS Lake API

[![tests](https://github.com/bmsuisse/lakeapi/actions/workflows/python-test.yml/badge.svg?branch=main)](https://github.com/bmsuisse/lakeapi/actions/workflows/python-test.yml)

#### Build on giants

<img src="/assets/LakeAPI.drawio.png" alt="LakeAPI" style="height: 100%; width:100%;"/>

A FastAPI Plugin that allows you to expose your Data Lake as an API, allowing multiple output formats, such as Parquet, Csv, Json, Excel, ...

The lake API also contains a minimal security layer for convenience (Basic Auth), but you can also bring your own.

It contrast to [roapi](https://github.com/roapi/roapi), we intentionally do not want to expose most SQL by default, but we limit possible queries using a config. This makes it easy for you to control what happens on your data. If you want the sql endpoint, you can enable this.

To run the app with default config, just do:

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

## OpenApi

Of course, everything works with Open API and FastAPI. Meaning you can add other FastAPI routes, you can use the /docs and /redoc endpoint.

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

The config file looks something like this, see also [our test yaml](config_test.yml):

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

## Partioning for awesome performance

In order to use partitions, you can either:

- partition by a column you filter on. Obviously
- partition on a special column called `columnname_md5_prefix_2` which means that you're partitioning by the first two chars of
  your hex-encoded md5 hash. If you now filter by `columnname` this will greatly reduce files searched for. The number of chars used is up to you, we found two to be meaningful
- partition on a special column called `columnname_md5_mod_NRPARTIIONS` where your partition value is `str(int(hashlib.md5(COLUMNNAME).hexdigest(), 16) % NRPARTITIONS)`. That might look a bit complicated, but it's not that hard :) your just doing a modulo on your md5 hash which
  allows you to set the exact number of partitions. Filtering is still happening on `columname` correctly

You must use deltalake to use parttions and you must only have str partition columns for now.

## Even more features

- Paging built-in, you can use limit/offset to control what you receive
- Full-text Search using DuckDB's Full Text Search Feature
- jsonify_complex Parameter to turn structs/lists into Json the client cannot deal with structs/lists
- Metadata endpoints to retrieve data types, string lengths and more
- Expose whole folders easily by using a "\*" wildcard in both the name and the datasource.uri config, see sample in above config
- Good test coverage
