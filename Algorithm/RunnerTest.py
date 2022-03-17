import sys
from Runners.UnionRunner import UnionRunner as curent

if __name__ == '__main__':
    arg = ''.join([i + ' ' for i in sys.argv[1:]])[:-1]
    a = curent()
    a.run()

