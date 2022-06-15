"""Классы для запуска алгоритмов"""
from Runners.Relational.UnionRunner import UnionRunner
from Algorithm.Relational.Difference import SymmetricDifference, Difference

class SymmetricDifferenceRunner(UnionRunner):

    def __init__(self, args: str = None, jobType: SymmetricDifference = SymmetricDifference, checkFileInput: bool = True):
        super(SymmetricDifferenceRunner, self).__init__(args, jobType, checkFileInput)

    def _setExtraArgs(self) -> None:
        super(SymmetricDifferenceRunner, self)._setExtraArgs()


    def _checkFile(self) -> bool:
        return super(SymmetricDifferenceRunner, self)._checkFile()


    def _checkOption(self) -> bool:
        return super(SymmetricDifferenceRunner, self)._checkOption()

    def _printInfo(self, key, value) -> str:
        return super(SymmetricDifferenceRunner, self)._printInfo(key, value)

    def _zeroPrintInfo(self) -> str:
        return super(SymmetricDifferenceRunner, self)._zeroPrintInfo()


class DifferenceRunner(SymmetricDifferenceRunner):

    def __init__(self, args: str = None, jobType: Difference = Difference, checkFileInput: bool = True):
        super(DifferenceRunner, self).__init__(args, jobType, checkFileInput)


    def _setExtraArgs(self) -> None:
        super(DifferenceRunner, self)._setExtraArgs()
        super()._setOption('--reduced', '')

    def _checkFile(self) -> bool:
        return super(DifferenceRunner, self)._checkFile()

    def _checkOption(self) -> bool:
        result = super(DifferenceRunner, self)._checkOption()
        if super()._getOption('--reduced')[0] not in super()._getInputFileNames():
            print("Incorrect '--reduced' args")
            result*=False
        return result

    def _printInfo(self, key, value) -> str:
        return super(DifferenceRunner, self)._printInfo(key, value)

    def _zeroPrintInfo(self) -> str:
        return super(DifferenceRunner, self)._zeroPrintInfo()