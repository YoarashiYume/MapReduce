
from Runners.Relational.UnionRunner import UnionRunner
from Algorithm.Relational.Join import RepartitionJoin
from pathlib import Path
from Struct.Table import Table

class JoinRunner(UnionRunner):

    def __init__(self, args: str = None, jobType: RepartitionJoin = RepartitionJoin, checkFileInput: bool = False):
        super(JoinRunner, self).__init__(args, jobType, checkFileInput)

    def _setExtraArgs(self) -> None:
        super(JoinRunner, self)._setExtraArgs()
        super()._setOption('--keys', '')


    def _checkFile(self) -> bool:
        #fileList = super()._getInputPaths()
        #table = Table(fileList[0], True, self._fileReader)
        #self._header = table.getHeader()
        #self._delimiter = table.getDelimiter()
        #self._headerSize = len(self._header)
        return True


    def _checkOption(self) -> bool:
        opt = super()._getOption('--keys')[0]
        opt =  list(map(str, opt.strip('[]').split(',')))
        files = super()._getInputPaths()
        if not len(opt) == len(files):
            print("Incorrect '--keys' args")
            return False
        newKey = '['
        result = True
        for key, path in zip(opt, files):
            if key not in Table(path,True,super()._fileReader).getHeader():
                print(f"No {key} in {path}")
                result *=False
            else:
                newKey+=Path(path).stem + ':'+key+','
                result *= True
        newKey=newKey[:-1]+']'
        super()._setOption('--keys', newKey)
        return result

    def _printInfo(self, key, value) -> str:
        value = value[1] + key + ','+value[1:-1] + value[-1]
        return super(JoinRunner, self)._printInfo(value,None)

    def _zeroPrintInfo(self) -> str:
        return str()