import abc
import typing

JobType = typing.TypeVar('JobType')
WriteType = typing.TypeVar('WriteType')


class Writer(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def __init__(self, **kwargs): pass
    @abc.abstractmethod
    def write(self, obj: typing.Any): pass
    @abc.abstractmethod
    def flush(self): pass


class Worker(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def __init__(self, **kwargs): pass
    @abc.abstractmethod
    def feed(self, obj: typing.Any): pass
    @abc.abstractmethod
    def flush(self): pass


class WriterProxy(Writer):
    def __init__(self, **kwargs): pass
    def write(self, obj: WriteType): pass
    def flush(self): pass


class WorkerProxy(Worker):
    def __init__(self, **kwargs): pass
    def feed(self, obj: typing.Any) -> typing.Any: pass
    def flush(self): pass
