import threading
from abc import ABC
from pyspark.sql import SparkSession
import datetime as dt

from com.db.fw.etl.core.common.Task import Task
import json



class BaseReader(Task):
    def __init__(self, task_name,type):
        Task.__init__(self, task_name,type)


class EvenHubsBatchReader(BaseReader):
    def __init__(self, task_name, type):
        Task.__init__(self, task_name, type)

    def execute(self):
        import datetime as dt
        ehConf = {}

        # Start from beginning of stream
        startOffset = "-1"

        # End at the current time. This datetime formatting creates the correct string format from a python datetime object
        endTime = dt.datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ")

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

        connectionString = "9PNftZRc3U3zYhjlvj401zz5lwbRUStGQuRJLmNUgAbxCBODwDusS0rkJpvMf7vCRnmnqXVYnfmZe1B8MOx8rKU+9OgVtZac5oUmRZhZrn3/puC6qLJpwK8KFxVrwlPVr5vTtQ//5bIRpaIPWg8KztzF2RtyAX4FQx6K421Cng+vcJn8hsjX0y0I0EwI+4kxTItdZYyFFAYVgoejQy+nxOLqxeQvdf3x9+mbPV4cXh3rQODnKHYCSdqrawXra/AI"

        ehConf['eventhubs.connectionString'] = connectionString
        ehConf["eventhubs.startingPosition"] = json.dumps(startingEventPosition)
        ehConf["eventhubs.endingPosition"] = json.dumps(endingEventPosition)
        ehConf["eventhubs.maxEventsPerTrigger"] = 2 * 5

        df = self.spark \
            .read \
            .format("eventhubs") \
            .options(**ehConf) \
            .load()

        self.set_output_dataframe(df)



