import json
from datetime import datetime

from com.db.fw.etl.core.common import Constants
import logging

from com.db.fw.etl.core.common.Task import Task


class Commons:

    DEBUG_PRINT_ON = False
    ERROR_PRINT_ON = False

    def get_curreny_time():
        x = datetime.now()
        curr_time = x.strftime("%Y-%m-%d %H:%M:%S.%f")
        return curr_time

    def printInfoMessage(self,message):
        if Task.DEBUG_ON:
            print(message)


    def printErrorMessage(self,message):
        if Task.ERROR_PRINT_ON:
            print(message)
