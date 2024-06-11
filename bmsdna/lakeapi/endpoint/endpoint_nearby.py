from pydantic import BaseModel
from bmsdna.lakeapi.context.df_base import ExecutionContext
import sqlglot.expressions as ex

from bmsdna.lakeapi.core.config import BasicConfig, Config
from bmsdna.lakeapi.core.model import GeoModel
from bmsdna.lakeapi.core.types import NearbyConfig
from sqlglot import select


NearbyType = list[tuple[GeoModel, NearbyConfig]] | None  # list of config with values


def parse_lat_lon(vl: str):
    lat, lon = vl.split(",")
    return float(lat), float(lon)


def _to_geo(v: dict | GeoModel):
    return GeoModel(**v) if isinstance(v, dict) else v


def get_nearby_filter(
    nearby_config: list[NearbyConfig],
    params: BaseModel,
    basic_config: BasicConfig,
) -> NearbyType:
    nearby_dict = {c.name.lower(): c for c in nearby_config}
    v = [
        (_to_geo(v), nearby_dict[k.lower()])
        for k, v in params.model_dump(exclude_unset=True).items()
        if k.lower() in nearby_dict and v is not None
    ]
    return v if len(v) > 0 else None


def handle_nearby_request(
    context: ExecutionContext,
    config: Config,
    params: BaseModel,
    basic_config: BasicConfig,
    *,
    source_view: str,
    query: ex.Query,
):
    if config.nearby is None:
        return query
    nearbyes = get_nearby_filter(
        config.nearby,
        params,
        basic_config,
    )
    if nearbyes is None:
        return query
    context.init_spatial()
    score_sum = None
    orders = []
    wheres = []
    for nearby_val, nearby_cfg in nearbyes:
        fn = context.distance_m_function(
            ex.column(nearby_cfg.lat_col, quoted=True),
            ex.column(nearby_cfg.lon_col, quoted=True),
            ex.convert(nearby_val.lat),
            ex.convert(nearby_val.lon),
        )
        query = query.select(fn.as_(nearby_cfg.name))
        orders.append(ex.column(nearby_cfg.name))
        wheres.append(ex.column(nearby_cfg.name) <= nearby_val.distance_m)

    if len(orders) > 0 or len(wheres) > 0:
        sel = select("*").from_(ex.to_identifier("nearbys"))
        sel.with_("nearbys", query, copy=False)

        for w in wheres:
            sel.where(w, append=True, copy=False)
        sel.order_by(*orders, copy=False)
        return sel

    return query
