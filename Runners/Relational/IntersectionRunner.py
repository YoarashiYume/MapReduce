
from Runners.Relational.UnionRunner import UnionRunner
from Algorithm.Relational.Intersection import Intersection
from Struct.BaseRunner import T

class IntersectionRunner(UnionRunner):

    def __init__(self, args: str = None, jobType: Intersection = Intersection, checkFileInput: bool = True):
        super(IntersectionRunner, self).__init__(args, jobType, checkFileInput)

    def _setExtraArgs(self) -> None:
        super(IntersectionRunner, self)._setExtraArgs()
        super()._setOption('--countOfTable', 2)

    def _checkFile(self) -> bool:
        return super(IntersectionRunner, self)._checkFile()

    def _checkOption(self) -> bool:
        countOfFiles = len(super()._getInputPaths())
        if not super()._getOption('--countOfTable') == countOfFiles:
            super()._setOption('--countOfTable', countOfFiles)
        return super(IntersectionRunner, self)._checkOption()

    def _printInfo(self, key, value) -> str:
        return super(IntersectionRunner, self)._printInfo(key, value)

    def _zeroPrintInfo(self) -> str:
        return super(IntersectionRunner, self)._zeroPrintInfo()
