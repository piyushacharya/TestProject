import json
from datetime import datetime


class Commons:

    DEBUG_PRINT_ON = False
    ERROR_PRINT_ON = False

    def get_curreny_time():
        x = datetime.now()
        curr_time = x.strftime("%Y-%m-%d %H:%M:%S.%f")
        return curr_time

    def printInfoMessage(message):
        if Commons.DEBUG_PRINT_ON :
            print(message)


    def printErrorMessage(message):
        if Commons.ERROR_PRINT_ON:
            print(message)
