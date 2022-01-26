from pathlib import Path
from .Union import Union, MRStep, List


class SymmetricDifference(Union):

    def __init__(self, args=None) -> None:
        super().__init__(args)

    def reducer(self, row: list, tableList: List[str]) -> None:
        if len(list(tableList)) == 1:
            yield row, None

    def steps(self) -> List[MRStep]:
        return [MRStep(mapper_raw=super().mapper_raw,
                       reducer=self.reducer)
                ]


class Difference(SymmetricDifference):
    __reducedTable: str = ''

    def __init__(self, args=None) -> None:
        super().__init__(args)

    def configure_args(self) -> None:
        super().configure_args()
        self.add_passthru_arg('--reduced', type=str, default='', help='name of reduced table')

    def load_args(self, args) -> None:
        super().load_args(args)
        if not len(self.options.reduced):
            raise "Incorrect args"
        self.__reducedTable = Path(self.options.reduced).stem

    def reducer(self, row: list, tableList: List[str]) -> None:
        tempList = set(tableList)
        if len(tempList) == 1 and self.__reducedTable in tempList:
            yield row, None
