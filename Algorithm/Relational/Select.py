from typing import List, Tuple
import re
from .Union import Union, Table, MRStep


class Select(Union):
    __selectedField: [str] = []
    __comparable: List[Tuple[str, str, int]] = []  # field, data,result

    def __init__(self, args=None) -> None:
        super().__init__(args)

    def configure_args(self) -> None:
        super(Select, self).configure_args()
        self.add_passthru_arg('--select', type=str, default='[*]', help='[arg1,arg2,...argN]')
        self.add_passthru_arg('--where', type=str, default='[]', help='[Arg1=value1,Arg2<value2,...ArgN=valueN]')

    def __compareResult(self, string: str) -> int:
        if string == '<': return -1
        if string == '=': return 0
        if string == '>': return 1
        raise Exception("Incorrect --where parameter")

    def load_args(self, args) -> None:
        super().load_args(args)
        self.__selectedField = list(map(str, self.options.select.strip('[]').split(',')))
        self.__comparable.clear()
        for el in list(map(str, self.options.where.strip('[]').split(','))):
            temp = re.split('([<>=])', el)
            if len(temp) == 3:
                self.__comparable.append((temp[0], temp[2], self.__compareResult(temp[1])))

    def __isfloat(self, string: str) -> bool:
        return re.match(r'^-?\d+(?:\.\d+)$', string)

    def __cmp(self, l, r) -> int:
        return (l > r) - (l < r)

    def __typeCmp(self, typename, l, r) -> int:
        return self.__cmp(typename(l), typename(r))

    def __checkRow(self, index, row: dict) -> bool:
        typename = str
        comparableData: str = self.__comparable[index][1]
        comparableFiled: str = row[self.__comparable[index][0]]
        if comparableData.isdigit() and comparableFiled.isdigit():
            typename = float if self.__isfloat(comparableData) or self.__isfloat(comparableFiled) else int
        return self.__typeCmp(typename, comparableFiled, comparableData) == self.__comparable[index][2]

    def __compareRow(self, row: dict) -> bool:
        if len(self.__comparable):
            for fieldIndex in range(0, len(self.__comparable)):
                if not self.__checkRow(fieldIndex, row):
                    return False
        return True


    def mapper_raw(self, input_path, input_uri) -> None:
        table = Table(path=input_path)
        try:
            while True:
                row = table.next()
                if not row:
                    break
                if self.__compareRow(row):
                    if self.__selectedField == ['*']:
                        yield list(map(lambda value: value[1], row.items())), None
                    else:
                        yield list(map(lambda value: value[1],
                                       filter(lambda value: value[0] in self.__selectedField, row.items()))), None
        finally:
            del table

    def steps(self) -> List[MRStep]:
        return [MRStep(mapper_raw=self.mapper_raw)]