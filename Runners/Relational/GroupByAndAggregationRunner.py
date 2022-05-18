from Runners.Relational.UnionRunner import UnionRunner
from Algorithm.Relational.GroupByAndAggregation import GroupByAndAggregation
from Struct.BaseRunner import T
from Struct.Table import Table


class GroupByAndAggregationRunner(UnionRunner):
    __headersList = None

    def __init__(self, args: str = None, jobType: GroupByAndAggregation = GroupByAndAggregation, checkFileInput: bool = True):
        super(GroupByAndAggregationRunner, self).__init__(args, jobType, checkFileInput)

    def _setExtraArgs(self) -> None:
        super(GroupByAndAggregationRunner, self)._setExtraArgs()
        super()._setOption('--groupBy', [])
        super()._setOption('--aggregationBy', [])
        super()._setOption('--aggregationFunction', '')
        super()._setOption('--printType', 'Full')

    def _checkFile(self) -> bool:
        result = super(GroupByAndAggregationRunner, self)._checkFile()
        return (len(self._header) and result)

    def __checkFunction(self) -> bool:
        result = True
        if super()._getOption('--aggregationFunction') == ['']:
            return result
        aggregationFunction = list(
            map(str, super()._getOption('--aggregationFunction')[0].strip('[]').split(',')))
        for el in aggregationFunction:
            if el not in ['avg', 'sum', 'max', 'min', 'count', 'range']:
                result *= False
        return result

    def _checkOption(self) -> bool:
        result = super(GroupByAndAggregationRunner, self)._checkOption()
        if not self.__checkFunction():
            print("Incorrect '--aggregationFunction' args")
            result *= False
        groupList = list(map(str, super()._getOption('--groupBy')[0].strip('[]').split(',')))
        if len(super()._getOption('--aggregationBy')):
            aggregationList = list(map(str, super()._getOption('--aggregationBy')[0].strip('[]').split(',')))
        else:
            aggregationList=[]
        aggregationFunction = list(
            map(str, super()._getOption('--aggregationFunction')[0].strip('[]').split(',')))
        aggregationDict = {}
        for i in range(0, len(aggregationList)):
            aggregationDict[aggregationList[i]] = aggregationFunction[0] if len(aggregationFunction) == 1 \
                else aggregationFunction[i] if i < len(aggregationFunction) else None

        for file in super()._getInputPaths():
            table = Table(file, True, super()._fileReader)
            if len(groupList) and not table.isHeadersInTable(groupList):
                print("Incorrect '--groupBy' args")
                result *= False
            if len(aggregationDict.keys()) and not table.isHeadersInTable(list(aggregationDict.keys())):
                print("Incorrect '--aggregationBy' args")
                result *= False
        return result

    def _printInfo(self, key, value) -> str:
        value = value[1] + key[1:-1] + ',' + value[1:-1] + value[-1]
        return super(GroupByAndAggregationRunner, self)._printInfo(value, None)

    def _zeroPrintInfo(self) -> str:
        return str()
