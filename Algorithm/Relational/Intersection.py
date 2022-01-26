from .Union import Union, MRStep, List


class Intersection(Union):
    __countOfTable: int = 2

    def __init__(self, args=None) -> None:
        super().__init__(args)

    def configure_args(self) -> None:
        super().configure_args()
        self.add_passthru_arg('--countOfTable', type=str, default='2', help='count of input tables')

    def load_args(self, args) -> None:
        super().load_args(args)
        if self.options.countOfTable.isdigit():
            self.__countOfTable = int(self.options.countOfTable)

    def reducer(self, row: list, tableList: list) -> None:
        if len(set(tableList)) == self.__countOfTable:
            yield row, None

    def steps(self) -> List[MRStep]:
        return [MRStep(mapper_raw=super().mapper_raw,
                       reducer=self.reducer)
                ]
