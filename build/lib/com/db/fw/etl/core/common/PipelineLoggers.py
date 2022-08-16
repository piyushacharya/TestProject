



class DeltaPipelineStatsLogger:
    print("delta logger")

    stats_table = "pipeline_stats"
    pipeline_status_table = "pipeline_status"

    tables_present = None
    database_name = None

    def __init__(self,spark):
        self.spark = spark


    def check_and_create(self):
        if DeltaPipelineStatsLogger.table_present == None:
            print("create database and tables as per schema")

    def storeOperationStats(self,stats):
        print("Save operation Stats ")

    def updateAndStoreStatus(self, status_obj):
        print("Save operation Stats ")


class RdbmsStatsLogger:
    conenction_param ={}
    connection=None
    tables_present = None
    #create connection Pool

    def check_and_create(self):
        if RdbmsStatsLogger.tables_present == None:
            print("create database and tables as per schema")

    def storeOperationStats(self,stats):
        print("Save operation Stats ")

    def updateAndStoreStatus(self, status_obj):
        print("Save operation Stats ")



    print("delta logger")
