from Runners.Relational.SelectRunner import SelectRunner
from Algorithm.Relational.Projection import Projection

class ProjectionRunner(SelectRunner):

    def __init__(self, args: str = None, jobType: Projection = Projection, checkFileInput: bool = True):
        super(ProjectionRunner, self).__init__(args, jobType, checkFileInput)

    def _setExtraArgs(self) -> None:
        super(ProjectionRunner, self)._setExtraArgs()

    def _checkFile(self) -> bool:
        return super(ProjectionRunner, self)._checkFile()

    def _checkOption(self) -> bool:
        return super(ProjectionRunner, self)._checkOption()

    def _printInfo(self, key, value) -> str:
        return super(ProjectionRunner, self)._printInfo(key, value)

    def _zeroPrintInfo(self) -> str:
        return super(ProjectionRunner, self)._zeroPrintInfo()