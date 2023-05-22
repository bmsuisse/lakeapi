# The BMS Lake API

A FastAPI Plugin that allows you to expose your Data Lake as an API, allowing multiple output formats, such as Parquet, Csv, Json, Excel, ...

The lake API also contains a minimal security layer for convenience (Basic Auth), but you can also bring your own.

It constrast to [roapi](https://github.com/roapi/roapi), we intentionally do not want to expose most SQL by default, but we limit possible queries using a config. This makes it easy for you to control what happens on your data. If you want the sql endpoint, you can enable this.

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
    dataframe:
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
    dataframe:
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
    dataframe:
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
    dataframe:
      uri: delta/fake
      file_type: delta

  - name: fruits_csv
    tag: test
    version: 1
    allow_get_all_pages: true
    api_method:
      - get
      - post
    params:
      - name: fruits
        operators:
          - "="
      - name: cars
        operators:
          - "="
    dataframe:
      uri: csv/fruits.csv
      file_type: csv
```
