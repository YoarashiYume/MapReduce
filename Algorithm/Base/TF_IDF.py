import os, re
from pathlib import Path
from typing import List, Dict, Tuple
from collections import defaultdict

from mrjob.job import MRJob
from mrjob.step import MRStep


class TFIDF(MRJob):
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

    def configure_args(self) -> None:
        super().configure_args()
        self.add_passthru_arg('--countOfFiles', type=str, default='', help='name of reduced table')

    def load_args(self, args) -> None:
        super().load_args(args)
        self.options.args = self.__dir2File(self.options.args)

    def mapper_raw_firstStep(self, input_path, input_uri) -> None:
        WORD_RE = re.compile(r"[\w']+")
        with open(input_path) as file:
            for line in file:
                for word in WORD_RE.findall(line):
                    yield Path(input_path).stem, tuple([word, 1])

    def __docWordCounter(self,docInfo: List[tuple]) -> dict:
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
            yield tuple([doc,word]), self.__tf(count,allWord)

    def __tf(self, wordRepetition: int, countOfWord: int) -> float:
        return wordRepetition / countOfWord



    def steps(self) -> List[MRStep]:
        return [MRStep(mapper_raw=self.mapper_raw_firstStep,
                       combiner=self.combiner_firstStep,
                       reducer=self.reducer_firstStep
                       )]
