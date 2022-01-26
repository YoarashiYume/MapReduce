import itertools
import re
from collections import defaultdict

from typing import List, Tuple, Dict

from mrjob.job import MRJob
from mrjob.step import MRStep


class CrossCorrelation(MRJob):
    WORD_RE = re.compile(r"[\w']+")

    def __init__(self, args=None) -> None:
        super().__init__(args)

    def mapper(self, _, line: str) -> None:
        for pair in itertools.combinations(sorted(self.WORD_RE.findall(line.lower())), 2):
            if not pair[0] == pair[1]:
                yield pair, 1

    def reducer(self, word: Tuple[str], values: List[int]) -> None:
        yield word, sum(values)

    def steps(self) -> List[MRStep]:
        return [MRStep(mapper=self.mapper,
                       combiner=self.reducer,
                       reducer=self.reducer)]


class CrossCorrelationMemUnDetect(MRJob):
    WORD_RE = re.compile(r"[\w']+")

    def __init__(self, args=None) -> None:
        super().__init__(args)

    def mapper(self, _, line: str) -> None:
        sortedList = sorted(self.WORD_RE.findall(line.lower()))
        for term in sortedList:
            dictionary: dict = defaultdict(int)
            for word in sortedList[sortedList.index(term):]:
                if not term == word:
                    dictionary[word] += 1
            yield term, dictionary

    def reducer(self, term: str, values: List[Dict[str, int]]) -> None:
        dictionary: dict = defaultdict(int)
        for _dict in values:
            for [word, _count] in _dict.items():
                dictionary[word] += _count
        for [word, _count] in dictionary.items():
            yield [term, word], _count

    def steps(self) -> List[MRStep]:
        return [MRStep(mapper=self.mapper,
                       reducer=self.reducer)]


class CrossCorrelationMemDetect(CrossCorrelationMemUnDetect):
    def __init__(self, args=None) -> None:
        super().__init__(args)

    def mapper(self, _, line: str) -> None:
        sortedList = sorted(self.WORD_RE.findall(line.lower()))
        for term in sortedList:
            dictionary: dict = defaultdict(int)
            for word in sortedList[sortedList.index(term):]:
                if not term == word:
                    try:
                        dictionary[word] += 1
                    except MemoryError:
                        yield term, dictionary
                        dictionary = defaultdict(int)
                        dictionary[word] = 1
            yield term, dictionary

    def combiner(self, term: str, values: List[Dict[str, int]]) -> None:
        dictionary: dict = defaultdict(int)
        for _dict in values:
            for [word, _count] in _dict.items():
                dictionary[word] += _count
        yield term, dictionary

    def steps(self) -> List[MRStep]:
        return [MRStep(mapper=self.mapper,
                       combiner=self.combiner,
                       reducer=super().reducer)]
