"""Код алгоритма"""
import itertools
from typing import List, Dict
from collections import defaultdict

from .Union import Union, Table, MRStep


class RepartitionJoin(Union):
    __TableKey: Dict[str, str] = {}

    def __init__(self, args=None) -> None:
        super().__init__(args)

    def configure_args(self) -> None:
        super().configure_args()
        self.add_passthru_arg('--keys', type=str, default={}, help='[firstTableKey,secondTableKey,...nTableKey]')

    def load_args(self, args):
        super().load_args(args)
        keyList = list(map(str, self.options.keys.strip('[]').split(',')))
        for el in keyList:
            [table, key] = el.split(':')
            self.__TableKey[table] = key

    def mapper_raw(self, input_path, input_uri) -> None:
        table = Table(path=input_path)
        if not table.isTableOpen():
            return  # or raise&
        try:
            while True:
                row = table.next()
                if not row:
                    break
                yield row.get(self.__TableKey[table.getTableName()]), \
                      tuple([table.getTableName(), list(map(lambda value: value[1],
                                                            filter(lambda value: value[0] != self.__TableKey[
                                                                table.getTableName()], row.items())))])  # tableName row
        finally:
            del table

    def reducer(self, key: str, argsList: List[tuple]) -> None:
        tables: Dict[str, list] = defaultdict(list)
        for table, _row in argsList:
            tables[table].append(_row)
        if len(tables) == len(self.__TableKey):
            result = list(itertools.product(*tables.values()))
            for el in result:
                yield key, [item for sublist in el for item in sublist]

    def steps(self) -> List[MRStep]:
        return [MRStep(mapper_raw=self.mapper_raw,
                       reducer=self.reducer)
                ]
