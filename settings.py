# -*- coding: utf-8 -*-
"""
Created on Mon Sep 10 15:30:31 2018

@author: Media
"""

import json
import os
from typing import Dict, Any


class Settings(object):
    __settings: Dict[str, Any] = {}

    @staticmethod
    def __open():
        if Settings.__settings:
            return Settings.__settings

        filename = os.path.join(os.path.split(__file__)[0],
                                'global.settings')
        if not os.path.exists(filename):
            raise FileExistsError('create settings file global.settings')

        try:
            with open(filename) as f:
                Settings.__settings = json.loads(f.read())
        except Exception:
            raise Exception("settings file global.settings corrupted")

        return Settings.__settings

    @staticmethod
    def ssl_dir():
        return Settings.__open()["ssl_dir"]

    @staticmethod
    def root_dir():
        return Settings.__open()["root_dir"]

    @staticmethod
    def host():
        return Settings.__open()["host"]

    @staticmethod
    def select_limit():
        return Settings.__open()["select_limit"]

    @staticmethod
    def years():
        return Settings.__open()["years"]

    @staticmethod
    def output_dir():
        return Settings.__open()["output_dir"]

    @staticmethod
    def models_dir():
        return Settings.__open()["models_dir"]

    @staticmethod
    def app_dir():
        return Settings.__open()["app_dir"]

    @staticmethod
    def log_filename():
        return Settings.__open()["log_filename"]
