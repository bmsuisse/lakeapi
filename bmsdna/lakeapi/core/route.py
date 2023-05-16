from typing import Literal, cast

from fastapi import APIRouter

from bmsdna.lakeapi.core.config import *
from bmsdna.lakeapi.core.dataframe import *
from bmsdna.lakeapi.core.endpoint import *
from bmsdna.lakeapi.core.env import CONFIG_PATH

logger = get_logger(__name__)


def init_routes(configs: Configs):
    from bmsdna.lakeapi.context.df_duckdb import DuckDbExecutionContext

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
                realdataframe = Dataframe(config.tag, config.name, config.dataframe, context)
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
                        "file_type": config.dataframe.file_type,
                        "uri": config.dataframe.uri,
                        "version": config.version,
                        "schema": {n: str(schema.field(n).type) for n in schema.names} if schema else None,
                    }
                )

            except Exception as err:
                logger.warning(f"Could not get response type for f{config.route}. Error:{err}")
                metamodel = None

            response_model = get_response_model(config=config, metamodel=metamodel) if metamodel is not None else None
            create_detailed_meta_endpoint(metamodel=metamodel, config=config, router=router)
            for am in methods:
                create_config_endpoint(
                    apimethod=am,
                    config=config,
                    router=router,
                    response_model=response_model,
                    metamodel=metamodel,
                )

        @router.get(
            "/metadata",
            name="metadata",
        )
        async def get_metadata(username: str = Depends(get_current_username)):
            return metadata

        create_sql_endpoint(router=router)

        return router
