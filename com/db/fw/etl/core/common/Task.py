import threading
from abc import ABC, abstractmethod
from datetime import datetime
from com.db.fw.etl.core.common.Commons import Commons
from com.db.fw.etl.core.Pipeline.PipelineGraph import PipelineNodeStatus
from com.db.fw.etl.core.common.Constants import COMMON_CONSTANTS as Constants


class Task(threading.Thread, ABC):

    def __init__(self, task_name, type):
        threading.Thread.__init__(self)
        self.task_name = task_name
        self.task_type = type
        self.spark = None
        self.logger = None
        self.pipeline_name = None
        self.pipeline_uid = None
        self.input_options = {}
        self.out_options = {}
        self.task_output_type = None;
        self.task_status = None
        self.task_facts = {}
        self.input_dataframes = {}
        self.input_dataframe = None
        self.input_table_name = {}
        self.output_dataframes = {}
        self.output_dataframe = None
        self.output_table_name = {}
        self.io_service = None
        self.error_msg = None

    def log(self, level, message):
        if self.logger is not None:
            message = "{}-{}-{}-{}-{}".format(" **ETL** ",self.pipeline_name,self.pipeline_uid,self.task_name,message)
            if level == Constants.INFO:
                self.logger.info(message)
            elif level == Constants.WARN:
                self.logger.warning(message)
            elif level == Constants.ERROR:
                self.logger.error(message)

    def reprJSON(self):
        return dict(task_name=self.task_name, type=self.task_type, pipeline_name=self.pipeline_name,
                    input_options=self.input_options, out_options=self.out_options,
                    task_output_type=self.task_output_type,
                    task_status=self.task_status, task_facts=self.task_facts)

    def add_option_value(self, values):
        self.input_options.update(values)

    def add_facts(self, fact_name, fact_value):
        self.task_facts[fact_name] = str(fact_value)

    def add_option_value(self, key, value):
        self.input_options[key] = value

    def add_option_values(self, input_options):
        self.input_options = input_options

    def get_option_value(self, key, value):
        self.input_options.get(key)

    def set_output_dataframe(self, df):
        self.output_dataframe = df

    def get_input_dataframe(self):
        return self.input_dataframe;

    def store_operation_stats(self):
        self.log(Constants.INFO,"Start store_operation_stats" )
        status_time = Commons.get_curreny_time()
        if len(self.task_facts) > 0:
            self.io_service.store_operational_stats(self.pipeline_uid, self.pipeline_name, self.task_name,
                                                    self.task_facts, status_time)
        self.log(Constants.INFO, "End store_operation_stats")

    def update_task_status(self, task_status):
        self.task_status = task_status

    def update_and_store_status(self, status, ex):
        self.log(Constants.INFO, "Start update_and_store_status")

        self.update_task_status(status)
        status_time = Commons.get_curreny_time()
        self.io_service.store_task_status(self.pipeline_name, self.pipeline_uid, self.task_name, self.task_status,
                                          status_time, ex)

        self.log(Constants.INFO, "End update_and_store_status")


    def run(self):
        try:
            self.log(Constants.INFO, "Start run")
            self.logger.info(
                "Task started {} ,status {} ,  at {} ".format(self.task_name, datetime.now, self.task_status))
            self.update_and_store_status(Constants.STARTED, None)
            self.log(Constants.INFO, "Start execute")
            self.execute()
            self.log(Constants.INFO, "End execute")
            self.store_operation_stats()
            self.update_and_store_status(Constants.FINISHED, None)
            self.logger.info(
                "Task started {} ,status {} ,  at {} ".format(self.task_name, datetime.now, self.task_status))
            self.log(Constants.INFO, "End run")
        except Exception as ex:
            self.logger.error("Task fail {} ".format(str(ex)))
            self.update_and_store_status(Constants.ERROR, ex)

    def encode(self):
        return vars(self)

    @abstractmethod
    def execute(self):
        pass
