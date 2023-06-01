import inspect
from typing import Literal, Tuple, cast

from fastapi import APIRouter, Depends, Request
from bmsdna.lakeapi.context import get_context_by_engine

from bmsdna.lakeapi.core.config import BasicConfig, Configs
from bmsdna.lakeapi.core.log import get_logger
from bmsdna.lakeapi.utils.fast_api_utils import _repeat_every


logger = get_logger(__name__)

all_lake_api_routers: list[Tuple[BasicConfig, Configs]] = []


def init_routes(configs: Configs, basic_config: BasicConfig):
    from bmsdna.lakeapi.context.df_duckdb import DuckDbExecutionContext

    from bmsdna.lakeapi.core.endpoint import (
        get_response_model,
        create_detailed_meta_endpoint,
        create_config_endpoint,
    )
    from bmsdna.lakeapi.core.sql_endpoint import create_sql_endpoint

    all_lake_api_routers.append((basic_config, configs))
    router = APIRouter()
    metadata = []

    with DuckDbExecutionContext() as context:
        for config in configs:
            methods = (
                cast(list[Literal["get", "post"]], [config.api_method])
                if isinstance(config.api_method, str)
                else config.api_method
            )
            try:
                from bmsdna.lakeapi.core.dataframe import Dataframe

                realdataframe = Dataframe(
                    config.version_str, config.tag, config.name, config.datasource, context, basic_config
                )
                if not realdataframe.file_exists():
                    logger.warning(
                        f"Could not get response type for f{config.route}. Path does not exist:{realdataframe.uri}"
                    )
                    metamodel = None
                else:
                    metamodel = realdataframe.get_df(endpoint="meta", partitions=None)
                schema = metamodel.arrow_schema() if metamodel else None
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
                logger.warning(f"Could not get response type for f{config.route}. Error:{err}")
                metamodel = None

            response_model = get_response_model(config=config, metamodel=metamodel) if metamodel is not None else None
            create_detailed_meta_endpoint(
                metamodel=metamodel, config=config, configs=configs, router=router, basic_config=basic_config
            )
            for am in methods:
                create_config_endpoint(
                    apimethod=am,
                    config=config,
                    router=router,
                    response_model=response_model,
                    metamodel=metamodel,
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

        @router.on_event("startup")
        @_repeat_every(seconds=60 * 60)  # 1 hour
        def _persist_search_endpoints() -> None:
            for config in configs:
                if config.search:
                    from bmsdna.lakeapi.core.dataframe import Dataframe

                    realdataframe = Dataframe(
                        config.version_str, config.tag, config.name, config.datasource, context, basic_config
                    )
                    if realdataframe.file_exists():
                        with get_context_by_engine(basic_config.default_engine) as ctx:
                            ctx.init_search(realdataframe.tablename, config.search)

        return router
