from Algorithm.File import WordCounter
from Struct.BaseRunner import BaseRunner


class WorldCounterRunner(BaseRunner):

    def __init__(self, args: str = None, jobType: WordCounter = WordCounter):
        super().__init__(args, jobType, False)

    def _setExtraArgs(self) -> None:
        return
