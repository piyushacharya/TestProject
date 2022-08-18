import random
import string

class COMMON_CONSTANTS:
    TABLE_NAME = "table_name"
    DB_NAME = "db_name"
    APPEND = "append"
    OVERWRITE = "overwrite"
    OPTIONS = "options"
    WHERE = "where"
    MERGE_CONDITION = "merge_condition"
    UPDATE_CONDITION = "update_condition"
    DELETE_CONDITION = "delete_condition"
    CHECK_POINT_LOCATION = "check_point_location"
    TRIGGER_TIME = "trigger_time"
    WRITER_TYPE= "writer_type"
    MERGE_CONDITION = "merge_condition"
    DO_UPDATE = "do_update"
    DO_DELETE = "do_delete"
    DO_INSERT = "do_insert"

    STARTED = "started"
    FINISHED = "finished"
    ERROR_FINISHED = "error_finished"
    ERROR = "error"

    INFO = "INFO"
    WARN = "WARN"





    def upper_Lower_random_string(length):
        result = ''.join(
            (random.choice(string.ascii_lowercase) for x in range(length)))
        return result


