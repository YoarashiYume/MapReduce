from .BaseRunner import BaseRunner
from Algorithm.Base.TF_IDF import TFIDF


class TFIDFRunner(BaseRunner):

    def __init__(self, args: str = None):
        super().__init__(TFIDF, args)

    def _setExtraArgs(self) -> None:
        if not super()._isOption('--countOfFiles'):
            super()._setOption('--countOfFiles', len(super()._getInputPaths()))

    def _checkFile(self) -> bool:
        return True
