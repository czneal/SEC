import inspect
import itertools

import indi.indprocs as procs
from utils import class_for_name

class A():
    def myfunc(self, args):
        print('Hello', args)
    
if __name__ == '__main__':
    print(inspect.getsource(A().__class__))