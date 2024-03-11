from typing import Optional, cast
import pyarrow as pa
import pypika
import pypika.queries as fn
from deltalake import DeltaTable, Metadata
from fastapi import (
    APIRouter,
    HTTPException,
    Query,
    Request,
)
from pydantic import BaseModel
from pypika.queries import QueryBuilder

from bmsdna.lakeapi.context import get_context_by_engine
from bmsdna.lakeapi.context.df_base import ResultData, get_sql
from bmsdna.lakeapi.core.config import BasicConfig, Config, Configs, Param, SearchConfig
from bmsdna.lakeapi.core.datasource import Datasource
from bmsdna.lakeapi.core.partition_utils import should_hide_colname
from bmsdna.lakeapi.core.types import Engines, MetadataDetailResult, MetadataSchemaField, MetadataSchemaFieldType

from typing import Optional


def _to_dict(tblmeta: Optional[Metadata]):
    if not tblmeta:
        return None
    return {
        "name": tblmeta.name,
        "description": tblmeta.description,
        "partition_columns": tblmeta.partition_columns,
        "configuration": tblmeta.configuration,
    }


def create_detailed_meta_endpoint(
    schema: Optional[pa.Schema],
    config: Config,
    configs: Configs,
    router: APIRouter,
    basic_config: BasicConfig,
):
    route = config.route + "/metadata_detail"
    has_complex = True
    if schema is not None:
        has_complex = any((pa.types.is_nested(t) for t in schema.types))

    @router.get(
        route,
        tags=["metadata", config.tag],
        operation_id=config.tag + "_" + config.name,
        name=config.name + "_metadata",
    )
    def get_detailed_metadata(
        req: Request,
        jsonify_complex: bool = Query(title="jsonify_complex", include_in_schema=has_complex, default=False),
        include_str_lengths: bool = True,
        engine: Engines | None = Query(
            title="$engine",
            alias="$engine",
            default=None,
            include_in_schema=False,
        ),
    ) -> MetadataDetailResult:
        import json

        req.state.lake_api_basic_config = basic_config
        from bmsdna.lakeapi.context import get_context_by_engine

        with get_context_by_engine(
            engine or config.engine or basic_config.default_engine, basic_config.default_chunk_size
        ) as context:
            assert config.datasource is not None
            realdataframe = Datasource(
                config.version_str,
                config.tag,
                config.name,
                config=config.datasource,
                sql_context=context,
                basic_config=basic_config,
                accounts=configs.accounts,
            )

            if not realdataframe.file_exists():
                raise HTTPException(404)
            partition_columns = []
            partition_values = None
            delta_tbl = None
            df = realdataframe.get_df(None)
            if config.datasource.file_type == "delta":
                delta_tbl = realdataframe.get_delta_table(schema_only=True)
                assert delta_tbl is not None
                partition_columns = delta_tbl.metadata().partition_columns
                partition_columns = [
                    c for c in partition_columns if not should_hide_colname(c)
                ]  # also hide those from metadata detail
                if len(partition_columns) > 0:
                    qb: QueryBuilder = (
                        df.query_builder().select(*[pypika.Field(c) for c in partition_columns]).distinct()
                    )
                    partition_values = context.execute_sql(qb).to_arrow_table().to_pylist()
            schema = df.arrow_schema()
            str_cols = [
                name
                for name in schema.names
                if (pa.types.is_string(schema.field(name).type) or pa.types.is_large_string(schema.field(name).type))
                and not should_hide_colname(name)
            ]
            complex_str_cols = (
                [
                    name
                    for name in schema.names
                    if (pa.types.is_struct(schema.field(name).type) or pa.types.is_list(schema.field(name).type))
                    and not should_hide_colname(name)
                ]
                if jsonify_complex
                else []
            )
            if include_str_lengths:
                str_lengths_query = (
                    pypika.Query.from_("strcols")
                    .with_(
                        context.jsonify_complex(df.query_builder(), complex_str_cols, str_cols + complex_str_cols),
                        "strcols",
                    )
                    .select(
                        *[
                            fn.Function("MAX", fn.Function(context.len_func, fn.Field(sc))).as_(sc)
                            for sc in str_cols + complex_str_cols
                        ]
                    )
                )
                str_lengths_df = (
                    (context.execute_sql(str_lengths_query).to_arrow_table().to_pylist())
                    if len(str_cols) > 0 or len(complex_str_cols) > 0
                    else [{}]
                )
                str_lengths = str_lengths_df[0]
            else:
                str_lengths = {}

            def _recursive_get_type(t: pa.DataType) -> MetadataSchemaFieldType:
                is_complex = pa.types.is_nested(t)

                return MetadataSchemaFieldType(
                    type_str=str(pa.string()) if is_complex and jsonify_complex else str(t),
                    orig_type_str=str(t),
                    fields=(
                        [
                            MetadataSchemaField(name=f.name, type=_recursive_get_type(f.type))
                            for f in [t.field(find) for find in range(0, cast(pa.StructType, t).num_fields)]
                        ]
                        if pa.types.is_struct(t) and not jsonify_complex
                        else None
                    ),
                    inner=(
                        _recursive_get_type(t.value_type)
                        if pa.types.is_list(t)
                        or pa.types.is_large_list(t)
                        or pa.types.is_fixed_size_list(t)
                        and t.value_type is not None
                        and not jsonify_complex
                        else None
                    ),
                )

            schema = df.arrow_schema()
            mdt = realdataframe.sql_context.get_modified_date(realdataframe.source_uri, realdataframe.config.file_type)
            return MetadataDetailResult(
                partition_values=partition_values,
                partition_columns=partition_columns,
                max_string_lengths=str_lengths,
                data_schema=[
                    MetadataSchemaField(
                        name=n, type=_recursive_get_type(schema.field(n).type), max_str_length=str_lengths.get(n, None)
                    )
                    for n in schema.names
                    if not should_hide_colname(n)
                ],
                delta_meta=_to_dict(delta_tbl.metadata() if delta_tbl else None),
                delta_schema=json.loads(delta_tbl.schema().to_json()) if delta_tbl else None,
                parameters=config.params,  # type: ignore
                search=config.search,
                modified_date=mdt,
            )
