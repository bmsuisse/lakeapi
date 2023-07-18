from typing import Literal, Tuple, cast

from fastapi import APIRouter, Depends, Request
from bmsdna.lakeapi.context import get_context_by_engine, ExecutionContext

from bmsdna.lakeapi.core.config import BasicConfig, Configs
from bmsdna.lakeapi.core.log import get_logger
from bmsdna.lakeapi.utils.fast_api_utils import _repeat_every


logger = get_logger(__name__)

all_lake_api_routers: list[Tuple[BasicConfig, Configs]] = []


def init_routes(configs: Configs, basic_config: BasicConfig):
    contexts: dict[str, ExecutionContext] = dict()

    def _get_context(name: str | None):
        real_name = name or basic_config.default_engine
        if not real_name in contexts:
            contexts[real_name] = get_context_by_engine(real_name, basic_config.default_chunk_size)
        return contexts[real_name]

    from bmsdna.lakeapi.core.endpoint import (
        get_response_model,
        create_config_endpoint,
    )
    from bmsdna.lakeapi.core.detail_endpoint import create_detailed_meta_endpoint
    from bmsdna.lakeapi.core.sql_endpoint import create_sql_endpoint

    all_lake_api_routers.append((basic_config, configs))
    router = APIRouter()
    metadata = []
    try:
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
                    config.datasource,
                    sql_context=_get_context(config.engine),
                    basic_config=basic_config,
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
                    from bmsdna.lakeapi.core.datasource import Datasource

                    assert config.datasource is not None
                    with get_context_by_engine(basic_config.default_engine, basic_config.default_chunk_size) as ctx:
                        realdataframe = Datasource(
                            config.version_str, config.tag, config.name, config.datasource, ctx, basic_config
                        )
                        if realdataframe.file_exists():
                            ctx.init_search(realdataframe.tablename, config.search)

        return router
    finally:
        for k, v in contexts.items():
            v.__exit__()
