from subprocess import Popen, PIPE


class FileReader:
    __out = None
    __streamHDFS = None
    __streamLocal = None

    def __init__(self, outer = None):
        self.__out = outer

    def __iter__(self):
        if self.__streamLocal:
            return self.__streamLocal.__iter__()
        elif self.__streamHDFS:
            return self.__streamHDFS.stdout.__iter__()

    def close(self):
        if self.__streamHDFS:
            self.__streamHDFS.kill()
        if self.__streamLocal:
            self.__streamLocal.close()

    def open(self, path: str) -> int:
        self.close()
        try:
            if not path.find(self.__out.HDFS_PREFIX, 0, len(self.__out.HDFS_PREFIX)):
                self.__streamHDFS = Popen(f'{self.__out.HDFS_PATH} dfs -cat {path}', shell=True, stdout=PIPE)
            else:
                self.__streamLocal = open(path, 'r')
            return 0
        except (FileExistsError, FileNotFoundError) as e:
            return e.args[0]

    def readline(self) -> str:
        if self.__streamLocal:
            return self.__streamLocal.readline()
        elif self.__streamHDFS:
            return self.__streamHDFS.stdout.readline().decode(self.__out.DECODE)

    def seek(self, pos: int):
        if self.__streamLocal:
            return self.__streamLocal.seek(pos)
        elif self.__streamHDFS:
            return self.__streamHDFS.stdout.seek(pos)

    def __del__(self):
        self.close()
