import atexit
from typing import List, Tuple
import contextlib

from indi.modclass import Classifier
from server import PipeManager, SERVER_PORT
from settings import Settings


class RemoteClassifier(Classifier):
    def __init__(self, fdict: str, model_id: int, max_len: int):
        super().__init__(fdict, model_id, max_len)

        m = PipeManager(address=(Settings.server_address(), SERVER_PORT))
        m.connect()
        self.pipe = m.get_pipe()
        atexit.register(self.close)

    def predict(self, pairs: List[Tuple[str, str]]) -> List[int]:
        res: List[int] = []
        for parent, child in pairs:
            self.pipe.send((parent, child, self.model_id))
            (_, _, _, r) = self.pipe.recv()
            res.append(r)
        return res

    def close(self):
        with contextlib.suppress(Exception):
            self.pipe.close()


def load_remote_classifiers() -> List[RemoteClassifier]:
    return [RemoteClassifier('d', i, 100) for i in range(8)]
