# define Python user-defined exceptions
class EtlException(Exception):
    def __init__(self,  task_name, pipeline_name, input_options,message = None ):
        self.message = "=> Task Name : {} ,   Pipeline Id : {} , Params : {} , Message {} ".format(task_name, pipeline_name,
                                                                             str(input_options), str (message))
        super().__init__(self.message)


class EtlReaderException(EtlException):
    def __init__(self, task_name, pipeline_name, input_options,message = None ):
        super().__init__(task_name, pipeline_name, input_options,message)


class EtlProcessorException(EtlException):
    def __init__(self, task_name, pipeline_name, input_options,message = None ):
        super().__init__(task_name, pipeline_name, input_options,message)


class EtlWriterException(EtlException):
    def __init__(self, task_name, pipeline_name, input_options,message = None ):
        super().__init__(task_name, pipeline_name, input_options,message)


class EtlDeltaWriterException(EtlWriterException):
    def __init__(self, task_name, pipeline_name, input_options,message = None ):
        super().__init__(task_name, pipeline_name, input_options,message)


class InsufficientWriterException(EtlWriterException):
    def __init__(self, task_name, pipeline_name, input_options,message = None ):
        super().__init__(task_name, pipeline_name, input_options,message)


class InsufficientParamsException(EtlException):
    def __init__(self, task_name, pipeline_name, input_options,message = None ):
        super().__init__(task_name, pipeline_name, input_options,message)


class InvalidParamsException(EtlException):
    def __init__(self, task_name, pipeline_name, input_options,message = None ):
        super().__init__(task_name, pipeline_name, input_options,message)


class PipelineException(Exception):
    def __init__(self, message):
        self.message = message
        super().__init__(self.message)


class EtlBuilderException(Exception):
    def __init__(self, message):
        self.message = message
        super().__init__(self.message)
