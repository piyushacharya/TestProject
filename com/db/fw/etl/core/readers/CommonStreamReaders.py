from com.db.fw.etl.core.common.Task import Task
from com.db.fw.etl.core.readers.CommonReaders import BaseReader


class EvenHubsReader(BaseReader):
    def __init__(self, task_name,type):
        Task.__init__(self, task_name,type)

    def execute(self):
        ehConf = {}
        # connectionString = self.get_option_value("YOUR.CONNECTION.STRING")
        # self.spark.sparkContext._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(
        #     connectionString)
        connectionString = """9PNftZRc3U3zYhjlvj401zz5lwbRUStGQuRJLmNUgAbxCBODwDusS0rkJpvMf7vCRnmnqXVYnfmZe1B8MOx8rKU+9OgVtZac5oUmRZhZrn3/puC6qLJpwK8KFxVrwlPVr5vTtQ//5bIRpaIPWg8KztzF2RtyAX4FQx6K421Cng+vcJn8hsjX0y0I0EwI+4kxTItdZYyFFAYVgoejQy+nxOLqxeQvdf3x9+mbPV4cXh3rQODnKHYCSdqrawXra/AI
"""

        ehConf['eventhubs.connectionString'] = connectionString
        df = self.spark\
            .readStream\
            .format("eventhubs")\
            .options(**ehConf)\
            .load()

        self.set_output_dataframe(df)



class JsonReader(BaseReader):
    def __init__(self, task_name,type):
        Task.__init__(self, task_name,type)

    def execute(self):
        pass
        # df = self.spark
        #     .readStream\
        #
        #     .schema()
        #     .json()
        #
        # self.set_output_dataframe(df)
