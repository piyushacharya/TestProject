from com.db.fw.etl.core.common.Task import Task
from com.db.fw.etl.core.readers.CommonReaders import BaseReader
from datetime import datetime as dt
import json


class EvenHubsReader(BaseReader):
    def __init__(self, task_name, type):
        Task.__init__(self, task_name, type)

    def execute(self):
        ehConf = {}

        # Start from beginning of stream
        startOffset = "-1"

        # End at the current time. This datetime formatting creates the correct string format from a python datetime object
        endTime = dt.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ")

        # Create the positions
        startingEventPosition = {
            "offset": startOffset,
            "seqNo": -1,  # not in use
            "enqueuedTime": None,  # not in use
            "isInclusive": True
        }

        endingEventPosition = {
            "offset": None,  # not in use
            "seqNo": -1,  # not in use
            "enqueuedTime": endTime,
            "isInclusive": True
        }

        # Put the positions into the Event Hub config dictionary

        # connectionString = "9PNftZRc3U3zYhjlvj401zz5lwbRUStGQuRJLmNUgAbxCBODwDusS0rkJpvMf7vCRnmnqXVYnfmZe1B8MOx8rKU+9OgVtZac5oUmRZhZrn3/puC6qLJpwK8KFxVrwlPVr5vTtQ//5bIRpaIPWg8KztzF2RtyAX4FQx6K421Cng+vcJn8hsjX0y0I0EwI+4kxTItdZYyFFAYVgoejQy+nxOLqxeQvdf3x9+mbPV4cXh3rQODnKHYCSdqrawXra/AI"

        connectionString = self.get_option_value("YOUR.CONNECTION.STRING")
        self.spark.sparkContext._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(
            connectionString)

        ehConf['eventhubs.connectionString'] = connectionString
        # ehConf["eventhubs.startingPosition"] = json.dumps(startingEventPosition)
        # ehConf["eventhubs.endingPosition"] = json.dumps(endingEventPosition)

        df = self.sparkStream \
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
