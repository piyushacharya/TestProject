import json
from datetime import datetime

from com.db.fw.etl.core.common import Constants
import logging


class Commons:


    def get_curreny_time():
        x = datetime.now()
        curr_time = x.strftime("%Y-%m-%d %H:%M:%S.%f")
        return curr_time

