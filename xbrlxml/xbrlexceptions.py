# -*- coding: utf-8 -*-
import json


class XbrlException(Exception):
    pass


class XbrlWarning(Exception):
    pass


class XBRLDictException(XbrlException):
    def __init__(self, exc_data):
        self.exc_data = exc_data
        XbrlException.__init__(self, str(self))

    def __str__(self):
        return json.dumps(self.exc_data, indent=3)

    def __repr__(self):
        return str(self.exc_data)
