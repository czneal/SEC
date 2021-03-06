import abc
import typing

# JobType = typing.TypeVar('JobType')
# WriteType = typing.TypeVar('WriteType')
JobType = typing.Any
WriteType = typing.Any


class Writer(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def write(self, obj: WriteType): pass
    @abc.abstractmethod
    def flush(self): pass


class Worker(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def feed(self, obj: JobType) -> WriteType: pass
    @abc.abstractmethod
    def flush(self): pass


class WriterProxy(Writer):
    def write(self, obj: WriteType): pass
    def flush(self): pass


class WorkerProxy(Worker):
    def feed(self, obj: JobType) -> WriteType: pass
    def flush(self): pass
