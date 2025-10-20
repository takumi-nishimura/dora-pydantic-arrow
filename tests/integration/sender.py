from dora import DoraStatus
from model import ExampleModel, IntKind, StrKind

from dora_pydantic_arrow import to_arrow


class Operator:
    def __init__(self):
        self.name = "Sender"
        self.frame_count = 0

    def on_event(self, dora_event, send_output) -> DoraStatus:
        self.frame_count += 1

        data = ExampleModel(
            str_kind=StrKind.EXAMPLE,
            int_kind=IntKind.FIRST,
            payload={"frame": self.frame_count},
        )
        send_output("frame_count", to_arrow([data]))

        return DoraStatus.CONTINUE
