from collections import defaultdict
from typing import List, Tuple, Dict, Set

from mrjob.job import MRJob
from mrjob.step import MRStep


class DistinctValue(MRJob):
    def __init__(self, args=None) -> None:
        super().__init__(args)

    def mapperFirstStep(self, _, line: str) -> None:
        value: str = line.split()[0]
        categoryList: List[str] = line.split()[1:]
        for category in categoryList:
            yield tuple([category, value]), None

    def reducerFirstStep(self, record: Tuple[str, str], _) -> None:
        yield record, None

    def mapperSecondStep(self, record: Tuple[str, str], _) -> None:
        yield record[0], 1

    def reducerSecondStep(self, category: str, timesList: List[int]) -> None:
        yield category, sum(timesList)

    def steps(self) -> List[MRStep]:
        return [MRStep(mapper=self.mapperFirstStep,
                       combiner=self.reducerFirstStep,
                       reducer=self.reducerFirstStep),
                MRStep(mapper=self.mapperSecondStep,
                       combiner=self.reducerSecondStep,
                       reducer=self.reducerSecondStep),
                ]


class DistinctValueMemUnDetect(DistinctValue):
    _dictionary: Dict[str, Set[str]]
    _categoryCounter: Dict[str, int] = {}

    def __init__(self, args=None) -> None:
        super().__init__(args)

    def mapper(self, _, line: str) -> None:
        value: str = line.split()[0]
        categoryList: List[str] = line.split()[1:]
        for category in categoryList:
            yield value, category

    def combiner_init(self) -> None:
        self._dictionary = defaultdict(Set[str])

    def combiner(self, key: str, categories: List[str]) -> None:
        self._dictionary[key] = set(categories)

    def combiner_final(self) -> None:
        for [key, categories] in self._dictionary.items():
            for category in categories:
                yield key, category

    def reducer_init(self) -> None:
        self._categoryCounter = defaultdict(int)

    def reducer(self, key: str, categories: List[str]) -> None:
        for category in set(categories):
            self._categoryCounter[category] += 1

    def reducer_final(self) -> None:
        for [key, count] in self._categoryCounter.items():
            yield key, count

    def reducerSecond(self, key: str, categories: List[int]) -> None:
        yield key, sum(categories)

    def steps(self) -> List[MRStep]:
        return [MRStep(mapper=self.mapper,
                       combiner_init=self.combiner_init,
                       combiner=self.combiner,
                       combiner_final=self.combiner_final,
                       reducer_init=self.reducer_init,
                       reducer=self.reducer,
                       reducer_final=self.reducer_final),
                MRStep(reducer=self.reducerSecond)]
