import csv
from typing import Optional, List, Any
from pathlib import Path

from .FileReader import FileReader


class Table:
    __file: Any = None
    __reader: type(csv.DictReader)
    __tableName: str
    __headers: List[str]
    __isOpen: bool = False
    __delimiter: str = str()

    def __init__(self, path: str, isHeader: bool = True, fReader: FileReader = None):
        try:
            if not fReader:
                self.__file = open(path)
            else:
                self.__file = fReader
                self.__file.open(path)
        except ...:
            self.__file.close()
            return
        self.__delimiter = csv.Sniffer().sniff(self.__file.readline()).delimiter
        self.__file.seek(0)
        self.__isOpen = True
        reader = csv.reader(self.__file, delimiter=self.__delimiter)
        if isHeader:
            self.__headers = next(reader)
        self.__file.seek(0)
        self.__reader = csv.DictReader(self.__file, delimiter=self.__delimiter)
        self.__tableName = Path(path).stem

    def __enter__(self):
        return self

    def getHeader(self):
        return self.__headers

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.__file.close()

    def isTableOpen(self) -> bool:
        return self.__isOpen

    def next(self) -> Optional[dict]:
        return next(self.__reader, None)

    def getTableName(self) -> str:
        return self.__tableName

    def getDelimiter(self):
        return self.__delimiter

    def isHeadersInTable(self, headers: List[str]) -> bool:
        return not len(list(filter(lambda head: head not in self.__headers, headers)))

    @staticmethod
    def strToList(string: str) -> list:
        delimiter = csv.Sniffer().sniff(string).delimiter
        return string.split(str(delimiter))

    def __del__(self):
        self.__file.close()
