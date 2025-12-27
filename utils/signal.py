from datetime import datetime
from pydantic import BaseModel, ConfigDict


class Signal(BaseModel):
    '''
    A Pydantic Model for validation and serialization of a Signal object.
    Signals represent the triggering of one node by another in the pipeline.
    '''
    model_config = ConfigDict(from_attributes=True)

    source_node_name: str
    source_run_id: int
    timestamp: datetime
    