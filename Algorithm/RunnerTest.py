import sys

from Runners.File.MeanCounterRunner import MeanCounterRunner as curent
from Algorithm.File.MeanCount import MeanCounterMemDetect

if __name__ == '__main__':
    arg = ''.join([i + ' ' for i in sys.argv[1:]])[:-1]
    a = curent(None,MeanCounterMemDetect)
    a.run()

