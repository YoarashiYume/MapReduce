import csv
from typing import Optional, List, TextIO
from pathlib import Path


class Table:
    __file: TextIO
    __reader: type(csv.DictReader)
    __tableName: str
    __headers: List[str]
    __isOpen: bool = False

    def __init__(self, path: str):
        try:
            self.__file = open(path)
        except ...:
            self.__file.close()
            return
        delimiter = csv.Sniffer().sniff(self.__file.readline()).delimiter
        self.__file.seek(0)
        self.__isOpen = True
        reader = csv.reader(self.__file, delimiter=delimiter)
        self.__headers = next(reader)
        self.__file.seek(0)
        self.__reader = csv.DictReader(self.__file, delimiter=delimiter)
        self.__tableName = Path(path).stem

    def isTableOpen(self) -> bool:
        return self.__isOpen

    def next(self) -> Optional[dict]:
        return next(self.__reader, None)

    def getTableName(self) -> str:
        return self.__tableName

    def isHeadersInTable(self, headers: List[str]) -> bool:
        return not len(list(filter(lambda head: head not in self.__headers, headers)))

    def __del__(self):
        self.__file.close()
