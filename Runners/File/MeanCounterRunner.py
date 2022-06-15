"""Класс для запуска алгоритмов"""
from Algorithm.File import MeanCount
from Struct.BaseRunner import BaseRunner


class MeanCounterRunner(BaseRunner):

    def __init__(self, args: str = None, jobType: MeanCount = MeanCount):
        super().__init__(args, jobType, False)

    def _setExtraArgs(self) -> None:
        return
