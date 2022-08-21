from com.db.fw.etl.core.Exception.EtlExceptions import InsufficientParamsException
from com.db.fw.etl.core.common.Task import Task
from com.db.fw.etl.core.readers.CommonReaders import BaseReader
from datetime import datetime as dt
from datetime import datetime, timedelta

import json


class EvenHubsReader(BaseReader):
    def __init__(self, task_name, type):
        Task.__init__(self, task_name, type)

    def execute(self):
        import datetime as dt
        ehConf = {}

        start_time = self.input_options.get("startingPosition", None)
        end_time = self.input_options.get("endingPosition", None)
        start_before = self.input_options.get("start_before_current_time_in_minutes", None)
        connectionString = str(self.input_options.get("connectionString", None))

        if connectionString is None:
            raise InsufficientParamsException(self.task_name, self.pipeline_name, self.input_options,
                                              "connectionString param is missing.. ")
        ehConf['eventhubs.connectionString'] = connectionString

        if start_time is not None:
            startingEventPosition = {
                "offset": start_time,
                "seqNo": -1,  # not in use
                "enqueuedTime": None,  # not in use
                "isInclusive": True
            }
            ehConf["eventhubs.startingPosition"] = json.dumps(startingEventPosition)

        if end_time is not None and start_time is not None:

            endingEventPosition = {
                "offset": None,  # not in use
                "seqNo": -1,  # not in use
                "enqueuedTime": end_time,
                "isInclusive": True
            }
            ehConf["eventhubs.endingPosition"] = json.dumps(endingEventPosition)
        elif start_time is not None:
            end_time = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ")
            endingEventPosition = {
                "offset": None,  # not in use
                "seqNo": -1,  # not in use
                "enqueuedTime": end_time,
                "isInclusive": True
            }
            ehConf["eventhubs.endingPosition"] = json.dumps(endingEventPosition)

        if start_before is not None:
            d = datetime.now() - timedelta(hours=0, minutes=start_before)
            start_time = d.strftime("%Y-%m-%dT%H:%M:%S.%fZ")

            endTime = dt.datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ")

            # Create the positions
            startingEventPosition = {
                "offset": None,
                "seqNo": -1,  # not in use
                "enqueuedTime": start_time,  # not in use
                "isInclusive": True
            }
            ehConf["eventhubs.startingPosition"] = json.dumps(startingEventPosition)

            endingEventPosition = {
                "offset": None,  # not in use
                "seqNo": -1,  # not in use
                "enqueuedTime": endTime,
                "isInclusive": True
            }
            ehConf["eventhubs.endingPosition"] = json.dumps(endingEventPosition)

        print("**** Configuration **** {}".format(str(ehConf)))

        df = self.spark \
            .readStream \
            .format("eventhubs") \
            .options(**ehConf) \
            .load()
        self.set_output_dataframe(df)


class JsonReader(BaseReader):
    def __init__(self, task_name, type):
        Task.__init__(self, task_name, type)

    def execute(self):
        pass
        # df = self.spark
        #     .readStream\
        #
        #     .schema()
        #     .json()
        #
        # self.set_output_dataframe(df)
