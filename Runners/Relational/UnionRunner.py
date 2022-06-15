"""Класс для запуска алгоритма"""
from Struct.BaseRunner import BaseRunner
from Algorithm.Relational.Union import Union
from Struct.Table import Table


class UnionRunner(BaseRunner):

    _header: list = []
    _headerSize: int = 0
    _delimiter: str = str()

    def __init__(self, args: str = None, jobType: Union = Union, checkFileInput: bool = True):
        super().__init__(args,jobType, checkFileInput)

    def _setExtraArgs(self) -> None:
        super()._setOption('--isHeader', len(self._header))

    def _checkFile(self) -> bool:
        fileList = super()._getInputPaths()
        table = Table(fileList[0], True, self._fileReader)
        self._header = table.getHeader()
        self._delimiter = table.getDelimiter()
        self._headerSize = len (self._header)
        result: bool = True
        for path in fileList[1:]:
            nextHeader = Table(path, True, self._fileReader).getHeader()
            if len(self._header):
                if not nextHeader == self._header:
                    self._header.clear()
            if not self._headerSize == len(nextHeader):
                result = False
                break
        self._fileReader.close()
        super()._setOption('--isHeader', len(self._header))
        return result


    def _printInfo(self, key, value) -> str:
        return self._delimiter.join(Table.strToList(key[1:-1]))

    def _zeroPrintInfo(self) -> str:
        return self._delimiter.join(self._header) if len(self._header) else str()