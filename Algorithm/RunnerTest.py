import sys

from Runners.File.MeanCounterRunner import MeanCounterRunner as curent
from Algorithm.File.MeanCount import WordCounterMemDetect

if __name__ == '__main__':
    arg = ''.join([i + ' ' for i in sys.argv[1:]])[:-1]
    a = curent(None,WordCounterMemDetect)
    a.run()

