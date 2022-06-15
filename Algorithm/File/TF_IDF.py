"""Код алгоритма"""
import re
from pathlib import Path
from typing import List, Tuple
from mrjob.job import MRJob
from mrjob.step import MRStep


class TFIDF(MRJob):

    def __init__(self, args=None) -> None:
        super().__init__(args)

    def configure_args(self) -> None:
        super().configure_args()
        self.add_passthru_arg('--countOfFiles', type=str, default='', help='count of files')

    def load_args(self, args) -> None:
        super().load_args(args)

    def mapper_raw_firstStep(self, input_path, input_uri) -> None:
        WORD_RE = re.compile(r"[\w']+")
        with open(input_path) as file:
            for line in file:
                for word in WORD_RE.findall(line):
                    yield Path(input_path).stem, tuple([word, 1])

    def __docWordCounter(self, docInfo: List[tuple]) -> dict:
        elList = list(docInfo)
        wordCount = {}
        for el in elList:
            count = sum(list(map(lambda value: value[1], filter(lambda value: value[0] == el[0], elList))))
            if not el[0] in wordCount:
                wordCount[el[0]] = count
        return wordCount

    def combiner_firstStep(self, doc: str, docInfo: List[tuple]) -> None:
        for word, count in self.__docWordCounter(docInfo).items():
            yield doc, tuple([word, count])

    def reducer_firstStep(self, doc: str, docInfo: List[tuple]) -> None:
        wordCount = self.__docWordCounter(docInfo)
        allWord = sum(wordCount.values())
        for word, count in wordCount.items():
            yield tuple([doc, word]), self.__tf(count, allWord)

    def __tf(self, wordRepetition: int, countOfWord: int) -> float:
        return wordRepetition / countOfWord

    def mapperSecondStep(self, docWord: tuple, tf: int) -> None:
        yield docWord[1], tuple([docWord[0], tf, 1])

    def reducerSecondStep(self, word: str, info: List[Tuple[str, int, int]]) -> None:
        infoList = list(info)
        countOfDoc = len(infoList)
        for el in infoList:
            yield tuple([word, el[0]]), tuple([el[1], countOfDoc])

    def _idf(self, docRepetition: int, allDocCount: int) -> float:
        return docRepetition / allDocCount

    def mapperThirdStep(self, docWord: tuple, tfCount: tuple) -> None:
        allDocCount = int(self.options.countOfFiles)
        yield docWord, tfCount[0] * self._idf(tfCount[1], allDocCount)

    def steps(self) -> List[MRStep]:
        return [MRStep(mapper_raw=self.mapper_raw_firstStep,
                       combiner=self.combiner_firstStep,
                       reducer=self.reducer_firstStep
                       ),
                MRStep(mapper=self.mapperSecondStep,
                       reducer=self.reducerSecondStep
                       ),
                MRStep(mapper=self.mapperThirdStep
                       )
                ]
