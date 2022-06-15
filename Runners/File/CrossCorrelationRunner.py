"""Класс для запуска алгоритмов"""
from Algorithm.File import CrossCorrelation
from Struct.BaseRunner import BaseRunner


class CrossCorrelationRunner(BaseRunner):

    def __init__(self, args: str = None, jobType: CrossCorrelation = CrossCorrelation):
        super().__init__(args, jobType, False)

    def _setExtraArgs(self) -> None:
        return
