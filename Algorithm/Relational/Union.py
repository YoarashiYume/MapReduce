"""Код алгоритма"""
from mrjob.job import MRJob
from mrjob.step import MRStep
from Struct.Table import Table, List


class Union(MRJob):
    def __init__(self, args=None) -> None:
        super().__init__(args)

    def configure_args(self) -> None:
        super().configure_args()
        self.add_passthru_arg('--isHeader', type=str, default='1', help='0 if file has no header, otherwise 1')

    def load_args(self, args):
        super().load_args(args)

    def mapper_raw(self, input_path, input_uri) -> None:
        with Table(path=input_path, isHeader=bool(self.options.isHeader)) as table:
            if table.isTableOpen():
                while True:
                    row = table.next()
                    if not row:
                        break
                    yield list(map(lambda value: value[1], row.items())), table.getTableName()

    def reducer(self, row: list, _):
        yield row, None

    def steps(self) -> List[MRStep]:
        return [MRStep(mapper_raw=self.mapper_raw,
                       reducer=self.reducer)
                ]
