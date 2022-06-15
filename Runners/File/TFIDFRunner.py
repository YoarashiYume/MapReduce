"""Класс для запуска алгоритма"""
from Struct.BaseRunner import BaseRunner
from Algorithm.File.TF_IDF import TFIDF


class TFIDFRunner(BaseRunner):

    def __init__(self, args: str = None):
        super().__init__(args,TFIDF, False)

    def _setExtraArgs(self) -> None:
        super()._setOption('--countOfFiles', 1)

    def _checkOption(self) -> bool:
        countOfFiles = len(super()._getInputPaths())
        super()._setOption('--countOfFiles', countOfFiles)
        return True