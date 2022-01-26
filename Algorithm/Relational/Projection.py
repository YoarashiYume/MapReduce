from .Select import Select, List, MRStep


class Projection(Select):

    def __init__(self, args=None) -> None:
        super().__init__(args)

    def reducer(self, row: list, _) -> None:
        yield row, None

    def steps(self) -> List[MRStep]:
        return [MRStep(mapper_raw=super().mapper_raw,
                       combiner=self.reducer,
                       reducer=self.reducer)
                ]
