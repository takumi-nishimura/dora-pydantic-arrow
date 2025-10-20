from typing import List

from dora import DoraStatus
from dora_pydantic_arrow import from_arrow
from model import ExampleModel


class Operator:
    def __init__(self):
        self.name = "Receiver"

    def on_event(self, dora_event, send_output) -> DoraStatus:
        if dora_event["type"] == "INPUT":
            if dora_event["id"] == "frame_count":
                arrow_array = dora_event["value"]
                data = from_arrow(arrow_array, type_hint=List[ExampleModel])
                print(f"Received data: {data}")

        return DoraStatus.CONTINUE
