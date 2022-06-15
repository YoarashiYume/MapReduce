"""Класс для запуска алгоритмов"""
from Algorithm.File import WordCounter
from Struct.BaseRunner import BaseRunner


class WordCounterRunner(BaseRunner):

    def __init__(self, args: str = None, jobType: WordCounter = WordCounter):
        super().__init__(args, jobType, False)

    def _setExtraArgs(self) -> None:
        return
