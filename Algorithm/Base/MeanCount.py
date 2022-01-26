from typing import List, Dict, Tuple

from mrjob.job import MRJob
from mrjob.step import MRStep


class MeanCounter(MRJob):
    def __init__(self, args=None) -> None:
        super().__init__(args)

    def _pairListWork(self, pairList: List[Tuple[float, int]]) -> Tuple[float, int]:
        _sum: float = 0
        _count: int = 0
        for [value, count] in pairList:
            _sum += value
            _count += count
        return _sum, _count

    def mapper(self, _, line: str) -> None:
        term: str = line.split()[0]
        valueList: List[str] = line.split()[1:]
        if not len(valueList):
            valueList.append('0')
        for value in valueList:
            try:
                yield term, tuple([float(value), 1])  # sum, count
            except ValueError:
                pass

    def combiner(self, term: str, pairList: List[Tuple[float, int]]) -> None:
        [_sum, _count] = self._pairListWork(pairList)
        yield term, tuple([_sum, _count])

    def reducer(self, term: str, pairList: List[Tuple[float, int]]) -> None:
        [_sum, _count] = self._pairListWork(pairList)
        avg: float = _sum / _count
        yield term, avg

    def steps(self) -> List[MRStep]:
        return [MRStep(mapper=self.mapper,
                       combiner=self.combiner,
                       reducer=self.reducer)]


class WordCounterMemUnDetect(MeanCounter):
    _sumDictionary: Dict[str, float]
    _countDictionary: Dict[str, int]

    def __init__(self, args=None) -> None:
        super().__init__(args)

    def mapper_init(self) -> None:
        self._sumDictionary = {}
        self._countDictionary = {}

    def mapper(self, _, line: str) -> None:
        term: str = line.split()[0]
        valueList: List[str] = line.split()[1:]
        if not len(valueList):
            valueList.append('0')
        for value in valueList:
            try:
                self._sumDictionary[term] = self._sumDictionary.get(term, 0) + float(value)
                self._countDictionary[term] = self._countDictionary.get(term, 0) + 1
            except ValueError:
                pass


    def mapper_final(self) -> None:
        for [term, count] in self._countDictionary.items():
            yield term, tuple([self._sumDictionary[term], count])  # sum, count

    def steps(self) -> List[MRStep]:
        return [MRStep(mapper_init=self.mapper_init,
                       mapper=self.mapper,
                       mapper_final=self.mapper_final,
                       reducer=super().reducer)]


class WordCounterMemDetect(WordCounterMemUnDetect):
    def __init__(self, args=None) -> None:
        super().__init__(args)

    def __mapYielder(self) -> None:
        super().mapper_final()
        super().mapper_init()

    def mapper(self, _, line: str) -> None:
        term: str = line.split()[0]
        valueList: List[str] = line.split()[1:]
        if not len(valueList):
            valueList.append('0')
        for value in valueList:
            fValue: float = 0
            try:
                fValue = float(value)
            except ValueError:
                pass
            try:
                self._sumDictionary[term] = self._sumDictionary.get(term, 0) + fValue
                self._countDictionary[term] = self._countDictionary.get(term, 0) + 1
            except MemoryError:
                self.__mapYielder()
                self._sumDictionary[term] = fValue
                self._countDictionary[term] = 1

    def steps(self) -> List[MRStep]:
        return [MRStep(mapper_init=super().mapper_init,
                       mapper=self.mapper,
                       mapper_final=super().mapper_final,
                       combiner=super().combiner,
                       reducer=super().reducer)]
