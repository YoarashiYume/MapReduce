import csv
from .BaseRunner import BaseRunner
from Algorithm.Relational.Union import Union
from Algorithm.Relational.Table import Table


class UnionRunner(BaseRunner):

    _header: list = []
    _headerSize: int = 0
    _delimiter: str = str()

    def __init__(self, args: str = None):
        super().__init__(Union, args, True)

    def _setExtraArgs(self) -> None:
        super()._setOption('--isHeader', len(self._header))

    def _checkFile(self) -> bool:
        fileList = super()._getInputPaths()
        fileReader: super()._fileReader = super()._fileReader(self)
        fileReader.open(fileList[0])
        header = fileReader.readLine()[:-1]
        self._delimiter = str(csv.Sniffer().sniff(header).delimiter)
        self._header = header.split(self._delimiter)
        self._headerSize = len(self._header)
        result: bool = True
        for path in fileList[1:]:
            fileReader.open(path)
            nextHeader = fileReader.readLine()[:-1].split(self._delimiter)
            if len(self._header):
                if not nextHeader == self._header:
                    self._header.clear()
            elif not self._headerSize == len(nextHeader):
                result = False
                break
        fileReader.close()
        return result


    def _printInfo(self, key, value) -> str:
        return self._delimiter.join(Table.strToList(key[1:-1]))

    def _zeroPrintInfo(self) -> str:
        return self._delimiter.join(self._header) if len(self._header) else str()