from pydantic import BaseModel
from datetime import datetime
import json

datetime.fromisoformat("2021-01-01")  # this works fine


class Model(BaseModel):
    dt: datetime


Model.model_validate_json(json.dumps({"dt": "2021-01-01T00:00"}))
Model.model_validate_json(json.dumps({"dt": "2021-01-01"}))
