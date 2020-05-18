# -*- coding: utf-8 -*-
"""
Created on Mon Sep 10 15:30:31 2018

@author: Media
"""

import json
import os
from typing import Dict, Any, cast


class Settings(object):
    __settings: Dict[str, Any] = {}

    @staticmethod
    def __open():
        if not Settings.__settings:
            Settings.reload()
        return Settings.__settings

    @staticmethod
    def reload() -> None:
        filename = os.path.join(os.path.split(__file__)[0],
                                'global.settings')
        if not os.path.exists(filename):
            raise FileExistsError('create settings file global.settings')

        try:
            with open(filename) as f:
                Settings.__settings = json.loads(f.read())
        except Exception:
            raise Exception("settings file global.settings corrupted")

    @staticmethod
    def ssl_dir():
        return Settings.__open()["ssl_dir"]

    @staticmethod
    def root_dir():
        return Settings.__open()["root_dir"]

    @staticmethod
    def host() -> str:
        return cast(str, Settings.__open()["host"])

    @staticmethod
    def port() -> int:
        return cast(int, Settings.__open()["port"])

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

    @staticmethod
    def n_proc() -> int:
        return cast(int, Settings.__open()["n_proc"])

    @staticmethod
    def n_proc_nasdaq() -> int:
        return cast(int, Settings.__open()['n_proc_nasdaq'])

    @staticmethod
    def server_address() -> str:
        return cast(str, Settings.__open()["server_address"])

    @staticmethod
    def form4_dir() -> str:
        return os.path.join(Settings.root_dir(), '3-4-5')

    @staticmethod
    def user() -> str:
        return cast(str, Settings.__open()["user"])

    @staticmethod
    def password() -> str:
        return cast(str, Settings.__open()["password"])
