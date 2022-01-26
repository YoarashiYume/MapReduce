import re
from typing import List, Dict
from collections import defaultdict
from mrjob.job import MRJob
from mrjob.step import MRStep


class WordCounter(MRJob):
    _WORD_RE = re.compile(r"[\w']+")

    def __init__(self, args=None) -> None:
        super().__init__(args)

    def mapper(self, _, line: str) -> None:
        for word in self._WORD_RE.findall(line):
            yield word.lower(), 1

    def reducer(self, word: str, values: list) -> None:
        yield word, sum(values)

    def steps(self) -> List[MRStep]:
        return [MRStep(mapper=self.mapper,
                       combiner=self.reducer,
                       reducer=self.reducer)]


class WordCounterMemUnDetect(WordCounter):
    _dictionary: Dict[str, int]

    def __init__(self, args=None) -> None:
        super().__init__(args)

    def mapper_init(self) -> None:
        self._dictionary = defaultdict(int)

    def mapper(self, _, line: str) -> None:
        for word in self._WORD_RE.findall(line):
            self._dictionary[word] += 1

    def mapper_final(self) -> None:
        for [word, count] in self._dictionary.items():
            yield word, count

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
        for word in self._WORD_RE.findall(line):
            try:
                self._dictionary[word] += 1
            except MemoryError:
                self.__mapYielder()
                self._dictionary[word] = 1

    def steps(self) -> List[MRStep]:
        return [MRStep(mapper_init=super().mapper_init,
                       mapper=self.mapper,
                       mapper_final=super().mapper_final,
                       combiner=super().reducer,
                       reducer=super().reducer)]
