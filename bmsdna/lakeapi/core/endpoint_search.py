from pydantic import BaseModel
from bmsdna.lakeapi.context.df_base import ExecutionContext
import pypika.queries
import pypika.terms
from bmsdna.lakeapi.core.types import SearchConfig

SearchesType = dict[str, tuple[str, SearchConfig]]


def get_searches(search_config: list[SearchConfig], params: BaseModel, min_search_length: int) -> SearchesType:
    search_dict = {c.name.lower(): c for c in search_config}
    return {
        k: (v, search_dict[k.lower()])
        for k, v in params.model_dump(exclude_unset=True).items()
        if k.lower() in search_dict and v is not None and len(v) >= min_search_length
    }


def handle_search_request(
    context: ExecutionContext,
    search_config: list[SearchConfig],
    source_view: str,
    query: pypika.queries.QueryBuilder,
    searches: SearchesType,
):
    context.init_search(source_view, search_config)
    score_sum = None
    for search_key, (search_val, search_cfg) in searches.items():
        score_sum = (
            context.search_score_function(source_view, search_val, search_cfg, alias=None)
            if score_sum is None
            else score_sum + context.search_score_function(source_view, search_val, search_cfg, alias=None)
        )
    assert score_sum is not None
    query = query.select(score_sum.as_("search_score"))
    query = query.where(pypika.terms.NotNullCriterion(pypika.queries.Field("search_score")))

    query = query.orderby(pypika.Field("search_score"), order=pypika.Order.desc)
    return query
