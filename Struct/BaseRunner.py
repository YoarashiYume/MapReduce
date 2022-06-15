
import os
import re
import ntpath
from urllib.parse import urlparse
import sys
from mrjob.job import MRJob
from pathlib import Path
from subprocess import Popen, PIPE
from typing import List, Dict, Generic, TypeVar
from collections import defaultdict
from mrjob.runner import MRJobRunner


from .FileReader import FileReader

import enum

T = TypeVar("T")


class BaseRunner(Generic[T]):


    class __parseState(enum.Enum):
        input = 'raw_input'
        output = 'output'
        optionPrefix = '--'

    COUNT_OF_HDFS_ARGS: int = 8
    HDFS_PATH: str = f'{os.getenv("HADOOP_HOME")}/bin/hdfs'
    HDFS_PREFIX = 'hdfs://'
    DECODE = 'utf-8'
    __options: Dict[str, List[str]] = defaultdict(list)
    __job: type = None
    _fileReader: FileReader = None

    def _isOption(self, opt: str) -> bool:
        return opt in self.__options

    def _setOption(self, opt: str, value):
        if issubclass(type(value), list):
            self.__options[opt] = [str(x) for x in value]
        else:
            res: list = [str(value)]
            self.__options[opt] = res

    def _checkOption(self) -> bool:
        return True

    def _getOption(self, opt: str) -> list:
        return self.__options[opt]

    def __getFileName(self, path):
        if path.find(self.HDFS_PREFIX, 0, len(self.HDFS_PREFIX)):
            a = urlparse(path)
            return os.path.basename(a.path)
        head, tail = ntpath.split(path)
        return tail or ntpath.basename(head)

    def _getInputFileNames(self):
        result = []
        for el in self._getInputPaths():
            result.append(self.__getFileName(el))
        return result

    def _getInputPaths(self):
        result = self.__options['local_input']
        result.extend(self.__options['hdfs_input'])
        return result

    def __cmdArgPars(self):
        return ''.join([i + ' ' for i in sys.argv[1:]])[:-1]

    def __init__(self, args: str = None, jobType: T = None, checkFileInput: bool = False):
        self.__job = jobType
        self._fileReader = FileReader(self)
        if not issubclass(self.__job, MRJob):
            raise Exception("Incorrect Type")
        self._setExtraArgs()
        self.__argsParser(args if args else self.__cmdArgPars())
        if checkFileInput:
            if not self._checkFile():
                raise Exception('File verification failed')
        if not self._checkOption():
            raise Exception('Option verification failed')


    def __optionParse(self, option: str) -> None:
        result: List[str] = option.split('=')
        if len(result) <= 1 and result[0].find(self.__parseState.optionPrefix.value, 0, len(self.__parseState.optionPrefix.value)):
            raise Exception(f"Incorrect option {option}")
        optHead = result.pop(0)
        self.__options[optHead] = [option[option.find('=')+1:]]

    def __argsParser(self, args: str) -> None:
        currentState = None
        for el in re.findall("\S*\[.*?\]|\S+", args):
            if el == '-i':
                currentState = self.__parseState.input.value
            elif el == '-o':
                currentState = self.__parseState.output.value
            elif not el.find(self.__parseState.optionPrefix.value, 0, len(self.__parseState.optionPrefix.value)):
                if el.find('[') == -1 ^ el.find(']') == -1:
                    raise Exception(f"Incorrect option {el}")
                self.__optionParse(el)
            else:
                self.__options[currentState].append(el)

        parse = self.__getFileList(self.__options['raw_input'])
        self.__options['local_input'] = parse[1]
        self.__options['hdfs_input'] = parse[0]

    def _setExtraArgs(self) -> None:
        raise NotImplementedError

    def _checkFile(self) -> bool:
        raise NotImplementedError

    def __getFileList(self, paths: List[str]) -> [list]:
        fileList: [list] = [[], []]
        for el in paths:
            if not el.find(self.HDFS_PREFIX, 0, len(self.HDFS_PREFIX)):
                fileList[0].extend(self.__getFileListFromHDFS(el))
            else:
                fileList[1].extend(self.__getFileListFromLocal(el))
        return fileList

    def __getFileListFromHDFS(self, path: str) -> List[str]:
        fileList: List[str] = []
        with Popen(f'{self.HDFS_PATH} dfs -ls {path}', shell=True, stdout=PIPE, stderr=PIPE) as process:
            std_out, std_err = process.communicate()
            if std_err:
                raise Exception(std_err.decode(self.DECODE))
            for el in std_out.decode(self.DECODE).splitlines():
                tempList = el.split()
                if len(tempList) == self.COUNT_OF_HDFS_ARGS:
                    if tempList[1].isdigit():
                        fileList.append(tempList[-1])
                    else:
                        fileList.extend(self.__getFileListFromHDFS(tempList[-1]))
        return fileList

    def __getFileListFromLocal(self, path: str) -> List[str]:
        fileList: List[str] = []
        if os.path.isdir(path):
            for root, d_names, f_names in os.walk(path):
                for f in f_names:
                    fileList.append(os.path.join(root, f))
        else:
            fileList.append(path)
        return fileList

    def __buildInput(self) -> list:
        result: list = self.__options['local_input']
        if len(self.__options['hdfs_input']):
            result.extend('-r hadoop'.split())
            result.extend(self.__options['hdfs_input'])
        return result

    def __buildOutput(self) -> list:
        result: list = []
        if self._isOption('output') and not len(Path(self.__options['output'][0]).suffix):
            result.append('-o')
            result.append(self.__options['output'][0])
        return result

    def __buildOption(self) -> list:
        result: list = []
        for key, value in self.__options.items():
            if not key.find(self.__parseState.optionPrefix.value, 0, len(self.__parseState.optionPrefix.value)):
                result.append(key + '=' + ''.join([i + ',' for i in value])[:-1])
        return result

    def __buildArgs(self) -> list:
        result = self.__buildInput()
        result.extend(self.__buildOutput())
        result.extend(self.__buildOption())
        return result

    def run(self) -> None:
        job = self.__job(self.__buildArgs())
        job.set_up_logging(stream=sys.stdout)
        with job.make_runner() as runner:
            runner.run()
            self.__outprint(runner)

    def _printInfo(self, key, value) -> str:
        return str(key) + '\t\t' + str(value)

    def _zeroPrintInfo(self) -> str:
        return str()

    def __outprint(self, runner: MRJobRunner) -> None:
        original = sys.stdout
        if self._isOption('output') and len(Path(self.__options['output'][0]).suffix):
            sys.stdout = open(self.__options['output'][0], 'w')
        if not self._isOption('output') or len(Path(self.__options['output'][0]).suffix):
            zeroLine:str = self._zeroPrintInfo()
            if len(zeroLine):
                print(zeroLine)
            for pair in runner.cat_output():
                if len(pair):
                    outputList = pair.decode(self.DECODE).split('\t')
                    print(self._printInfo(outputList[0], outputList[-1][:-1]))
        if not sys.stdout == original:
            sys.stdout.close()
            sys.stdout = original
