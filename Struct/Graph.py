import re
from math import inf
from typing import TextIO, Optional, Dict, List, Any

from FileReader import FileReader



class Node:
    name: str = ''
    distance: int = 0
    paths: Dict[str, int] = {} # where, costs


class Graph:
    __file: Any = None
    __isOpen: bool = False
    __isList: bool = False
    __initialNodeName: str = None
    __Header: List[str] = []

    def __init__(self, path: str, initialDot: str, fReader: FileReader = None):
        try:
            if not fReader:
                self.__file = open(path)
            else:
                self.__file = fReader
            self.__isList = self.__determinateType()
        except ...:
            self.__file.close()
            return
        __initialNodeName = initialDot
        self.__isOpen = True
        if not self.__isList:
            self.__loadHeader()

    def __loadHeader(self) -> None:
        string: str = self.__file.readline()
        self.__Header = [el for el in string.split()]

    def __determinateType(self) -> bool:
        string: str = self.__file.readline()
        self.__file.seek(0)
        result: bool = False
        for el in [':', '-']:
            if string.find(el) != -1:
                result = True
                break
        return result

    def __del__(self):
        self.__file.close()

    def isGraphOpen(self) -> bool:
        return self.__isOpen

    def getNode(self) -> Optional[Node]:
        string: str = self.__file.readline()
        result = None
        if len(string):
            result = Node()
            splitList = re.compile(r"[\w']+").findall(string)
            result.name = splitList.pop(0)
            result.distance = 0 if result.name == self.__initialNodeName else int(inf)
            try:
                if not self.__isList:
                    for index in range(0, len(self.__Header)):
                        if int(splitList[index]):
                            result.paths[self.__Header[index]] = int(splitList[index])
                else:
                    for el in splitList:
                        result.paths[el] = 1
            except:
                self.__del__()
                result = None
                if not result:
                    raise Exception('Unexpected format')
        return result
