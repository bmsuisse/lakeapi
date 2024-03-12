from typing import Literal, Tuple, cast

from fastapi import APIRouter, Depends, Request
from bmsdna.lakeapi.context import get_context_by_engine, ExecutionContext, ExecutionContextManager

from bmsdna.lakeapi.core.config import BasicConfig, Configs
from bmsdna.lakeapi.core.log import get_logger
from bmsdna.lakeapi.utils.fast_api_utils import _repeat_every


logger = get_logger(__name__)

all_lake_api_routers: list[Tuple[BasicConfig, Configs]] = []


def init_routes(configs: Configs, basic_config: BasicConfig):
    from bmsdna.lakeapi.endpoint.endpoint import (
        get_response_model,
        create_config_endpoint,
    )
    from bmsdna.lakeapi.endpoint.detail_endpoint import create_detailed_meta_endpoint
    from bmsdna.lakeapi.endpoint.sql_endpoint import create_sql_endpoint
    from bmsdna.lakeapi.core.schema_cache import get_schema_cached

    all_lake_api_routers.append((basic_config, configs))
    router = APIRouter()
    metadata = []
    with ExecutionContextManager(
        basic_config.default_engine,
        basic_config.default_chunk_size,
    ) as mgr:
        for config in configs:
            methods = (
                cast(list[Literal["get", "post"]], [config.api_method])
                if isinstance(config.api_method, str)
                else config.api_method
            )
            try:
                from bmsdna.lakeapi.core.datasource import Datasource

                assert config.datasource is not None
                realdataframe = Datasource(
                    config.version_str,
                    config.tag,
                    config.name,
                    config=config.datasource,
                    sql_context=mgr.get_context(config.engine),
                    basic_config=basic_config,
                    accounts=configs.accounts,
                )
                schema = get_schema_cached(basic_config, realdataframe, config.datasource.get_unique_hash())
                if schema is None:
                    logger.warning(
                        f"Could not get response type for f{config.route}. Path does not exist:{realdataframe}"
                    )
                metadata.append(
                    {
                        "name": config.name,
                        "tag": config.tag,
                        "route": config.route,
                        "methods": methods,
                        "file_type": config.datasource.file_type,
                        "uri": config.datasource.uri,
                        "version": config.version,
                        "schema": {n: str(schema.field(n).type) for n in schema.names} if schema else None,
                    }
                )

            except Exception as err:
                import traceback

                print(traceback.format_exc())
                logger.warning(f"Could not get response type for f{config.route}. Error:{err}")
                schema = None

            response_model = (
                get_response_model(
                    config=config,
                    schema=schema,
                )
                if schema is not None
                else None
            )
            create_detailed_meta_endpoint(
                schema=schema,
                config=config,
                configs=configs,
                router=router,
                basic_config=basic_config,
            )
            for am in methods:
                create_config_endpoint(
                    apimethod=am,
                    config=config,
                    router=router,
                    response_model=response_model,
                    schema=schema,
                    basic_config=basic_config,
                    configs=configs,
                )

        @router.get(
            "/metadata",
            name="metadata",
        )
        async def get_metadata():
            return metadata

        if basic_config.enable_sql_endpoint:
            create_sql_endpoint(
                router=router,
                basic_config=basic_config,
                configs=configs,
            )

        return router
