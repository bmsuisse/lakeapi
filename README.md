# The BMS Lake API

A FastAPI Plugin that allows you to expose your Data Lake as an API, allowing multiple output formats, such as Parquet, Csv, Json, Excel, ...

It also contains a minimal security layer for convenience (Basic Auth), but you can also bring your own.

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
