from Algorithm.File import DistinctValue
from Struct.BaseRunner import BaseRunner


class DistinctValueRunner(BaseRunner):

    def __init__(self, args: str = None, jobType: DistinctValue = DistinctValue):
        super().__init__(args, jobType, False)

    def _setExtraArgs(self) -> None:
        return
