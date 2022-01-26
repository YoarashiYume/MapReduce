import os
from mrjob.job import MRJob
from mrjob.step import MRStep
from .Table import Table, List


class Union(MRJob):
    def __init__(self, args=None) -> None:
        super().__init__(args)

    def __dir2File(self, args: List[str]) -> List[str]:
        fileList: List[str] = []
        for el in args:
            if os.path.isdir(el):
                for root, d_names, f_names in os.walk(el):
                    for f in f_names:
                        fileList.append(os.path.join(root, f))
            else:
                fileList.append(el)
        return fileList

    def load_args(self, args):
        super().load_args(args)
        self.options.args = self.__dir2File(self.options.args)

    def mapper_raw(self, input_path, input_uri) -> None:
        table = Table(path=input_path)
        if not table.isTableOpen():
            return  # or raise&
        try:
            while True:
                row = table.next()
                if not row:
                    break
                yield list(map(lambda value: value[1], row.items())), table.getTableName()
        finally:
            del table

    def reducer(self, row: list, _):
        yield row, None

    def steps(self) -> List[MRStep]:
        return [MRStep(mapper_raw=self.mapper_raw,
                       reducer=self.reducer)
                ]
