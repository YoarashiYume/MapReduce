"""Класс для запуска алгоритма"""
import re
from Runners.Relational.UnionRunner import UnionRunner
from Algorithm.Relational.Select import Select


class SelectRunner(UnionRunner):

    def __init__(self, args: str = None, jobType: Select = Select, checkFileInput: bool = True):
        super(SelectRunner, self).__init__(args, jobType, checkFileInput)

    def _setExtraArgs(self) -> None:
        super(SelectRunner, self)._setExtraArgs()
        super()._setOption('--select', '[*]')
        super()._setOption('--where', '[]')

    def _checkFile(self) -> bool:
        result = super(SelectRunner, self)._checkFile()
        return (len(self._header) and result)

    def _checkOption(self) -> bool:
        result = super(SelectRunner, self)._checkOption()
        opt = super()._getOption('--select')
        if not opt == ['[*]']:
            for el in opt:
                part = list(map(str, el.strip('[]').split(',')))
                for item in part:
                    if item not in self._header:
                        print("Incorrect '--select' args")
                        result *= False
        else:
            result *= True

        opt = super()._getOption('--where')
        if not opt == ['[]']:
            for el in opt:
                part = list(map(str, el.strip('[]').split(',')))
                for item in part:
                    temp = re.split(r'([<>=]+)', item)
                    if not len(temp) == 3 or temp[0] not in self._header or (not len(temp[1]) == 1 or temp[1] not in ['>', '<', '=']):
                        print("Incorrect '--where' args")
                        result *= False
        else:
            result *= True

        return result

    def _printInfo(self, key, value) -> str:
        return super(SelectRunner, self)._printInfo(key, value)

    def _zeroPrintInfo(self) -> str:
        return super(SelectRunner, self)._zeroPrintInfo()