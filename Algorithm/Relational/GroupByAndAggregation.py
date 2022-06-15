"""Код алгоритма"""
from typing import List, Dict
from .Union import Union, Table, MRStep


class GroupByAndAggregation(Union):
    __groupList: List[str] = []
    __aggregationDict: Dict = {}

    def __init__(self, args=None) -> None:
        super().__init__(args)

    def configure_args(self) -> None:
        super().configure_args()
        self.add_passthru_arg('--groupBy', type=str, default={}, help='[arg1,arg2,...argN]')
        self.add_passthru_arg('--aggregationBy', type=str, default={}, help='[Arg1,Arg2,...ArgN]')
        self.add_passthru_arg('--aggregationFunction', type=str, default=None, help='avg,sum,max,min,count,range')
        self.add_passthru_arg('--printType', type=str, default='Full', help='Full/Grouped')

    def __avg(self, rows: List[dict], arg: str) -> str:
        __sum = sum(map(lambda x: int(x[arg]), rows))
        return str(__sum / len(rows))

    def __sum(self, rows: List[dict], arg: str) -> str:
        __sum = sum(map(lambda x: int(x[arg]), rows))
        return str(__sum)

    def __max(self, rows: List[dict], arg: str) -> str:
        __max = max(map(lambda x: int(x[arg]), rows))
        return str(__max)

    def __min(self, rows: List[dict], arg: str) -> str:
        __min = min(map(lambda x: int(x[arg]), rows))
        return str(__min)

    def __count(self, rows: List[dict], arg: str) -> str:
        return str(len(rows))

    def __range(self, rows: List[dict], arg: str) -> str:
        return str(int(self.__max(rows, arg)) - int(self.__min(rows, arg)))

    def __getAggregation(self, funcName: str):
        funcName = funcName.lower()
        if funcName == 'avg': return self.__avg
        if funcName == 'sum': return self.__sum
        if funcName == 'max': return self.__max
        if funcName == 'min': return self.__min
        if funcName == 'count': return self.__count
        if funcName == 'range': return self.__range
        return None

    def load_args(self, args):
        super().load_args(args)
        self.__groupList = list(map(str, self.options.groupBy.strip('[]').split(',')))
        aggregationList = list(map(str, self.options.aggregationBy.strip('[]').split(',')))
        aggregationFunction = list(map(str, self.options.aggregationFunction.strip('[]').split(',')))
        for i in range(0, len(aggregationList)):
            self.__aggregationDict[aggregationList[i]] = aggregationFunction[0] if len(aggregationFunction) == 1 \
                else aggregationFunction[i] if i < len(aggregationFunction) else None

    def mapper_raw(self, input_path, input_uri) -> None:
        table = Table(path=input_path)
        if table.isTableOpen():
            try:
                while True:
                    row = table.next()
                    if not row:
                        break
                    yield list(map(lambda value: value[1],
                               filter(lambda value: value[0] in self.__groupList, row.items()))), row
            finally:
                del table

    def __aggrigationFunc(self, row: List[dict]) -> dict:
        result: dict = {}
        for arg, fooName in self.__aggregationDict.items():
            foo = self.__getAggregation(fooName)
            if foo is not None:
                result[arg] = foo(row, arg)
        return result

    def reducerFull(self, _, rows: List[dict]) -> None:
        rowList = list(rows)
        result = self.__aggrigationFunc(rowList)
        for el in rowList:
            yield list(map(lambda value: value[1], el.items())), list(map(lambda value: value[1], result.items()))

    def reducerGroup(self, groupedList: list, rows: List[dict]) -> None:
        rowList = list(rows)
        yield groupedList, list(map(lambda value: value[1], self.__aggrigationFunc(rowList).items()))

    def steps(self) -> List[MRStep]:
        return [MRStep(mapper_raw=self.mapper_raw,
                       reducer=self.reducerFull if self.options.printType == 'Full' else self.reducerGroup)
                ]
