from pydantic import BaseModel
from bmsdna.lakeapi.context.df_base import ExecutionContext
import pypika.queries
import pypika.terms
from bmsdna.lakeapi.core.config import BasicConfig, Config
from bmsdna.lakeapi.core.types import SearchConfig

SearchesType = list[tuple[str, SearchConfig]] | None  # list of config with values


def get_searches(
    search_config: list[SearchConfig],
    params: BaseModel,
    basic_config: BasicConfig,
) -> SearchesType:
    search_dict = {c.name.lower(): c for c in search_config}
    v = [
        (v, search_dict[k.lower()])
        for k, v in params.model_dump(exclude_unset=True).items()
        if k.lower() in search_dict and v is not None and len(v) >= basic_config.min_search_length
    ]
    return v if len(v) > 0 else None


def handle_search_request(
    context: ExecutionContext,
    config: Config,
    params: BaseModel,
    basic_config: BasicConfig,
    *,
    source_view: str,
    query: pypika.queries.QueryBuilder
):
    if config.search is None:
        return query
    searches = get_searches(config.search, params, basic_config)
    if searches is None:
        return query
    context.init_search(source_view, config.search)
    score_sum = None

    for search_val, search_cfg in searches:
        score_sum = (
            context.search_score_function(source_view, search_val, search_cfg, alias=None)
            if score_sum is None
            else score_sum + context.search_score_function(source_view, search_val, search_cfg, alias=None)
        )
    assert score_sum is not None
    query = query.select(score_sum.as_("search_score"))
    query = query.where(pypika.terms.NotNullCriterion(pypika.queries.Field("search_score")))
    query._orderbys = []  # reset order
    query = query.orderby(pypika.Field("search_score"), order=pypika.Order.desc)
    return query
