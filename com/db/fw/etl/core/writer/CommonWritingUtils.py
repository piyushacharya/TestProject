from com.db.fw.etl.core.common.Constants import *
from pyspark.sql import SparkSession


def delta_insert(spark, df, options, mode):
    db_name = options.get(COMMON_CONSTANTS.DB_NAME)
    table_name = options.get(COMMON_CONSTANTS.TABLE_NAME)
    input_options = options.get(COMMON_CONSTANTS.OPTIONS)
    writer_type = options.get(COMMON_CONSTANTS.WRITER_TYPE)

    print ("delta_insert {} ".format(str(options)))

    if writer_type is not None and writer_type.upper() == "STREAM":

        print("******** Inside Stream ******")

        df_writer = df.writeStream \
            .format("delta") \
            .outputMode("append")

        check_point = options.get(COMMON_CONSTANTS.CHECK_POINT_LOCATION, None)
        if check_point is not None:
            df_writer = df_writer.option("checkpointLocation", check_point)

        trigger_time = options.get(COMMON_CONSTANTS.TRIGGER_TIME, None)
        if trigger_time is not None:
            df_writer = df_writer.trigger(processingTime=str(trigger_time))

        df_writer.toTable("{}.{}".format(db_name, table_name))

    else:

        print(f"******** Inside Batch ****** {db_name} {table_name} {mode}")

        df_writer = df.write \
            .format("delta") \
            .mode(mode)

        # if COMMON_CONSTANTS.OPTIONS in options.keys():
        #     df_writer = df_writer.options(input_options)

        df_writer.option("mergeSchema", "true").saveAsTable("{}.{}".format(db_name, table_name))


def delta_delete(spark, df, options):
    db_name = options.get(COMMON_CONSTANTS.DB_NAME)
    table_name = options.get(COMMON_CONSTANTS.TABLE_NAME)

    where_condition = options.get(COMMON_CONSTANTS.WHERE)
    sql = "DELETE FROM {}.{} ".format(db_name, table_name)

    if COMMON_CONSTANTS.WHERE in options.keys():
        sql = sql + "Where {}".format(where_condition)

    spark.sql(sql)


def delta_update(spark, df, options):
    print("go for update ")


"""
    MERGE INTO target_table_name [target_alias]
    USING source_table_reference [source_alias]
    ON merge_condition
    [ WHEN MATCHED [ AND condition ] THEN matched_action ] [...]
    [ WHEN NOT MATCHED [ AND condition ]  THEN not_matched_action ] [...]

    matched_action
    { DELETE |
    UPDATE SET * |
    UPDATE SET { column1 = value1 } [, ...] }

    not_matched_action
    { INSERT * |
    INSERT (column1 [, ...] ) VALUES (value1 [, ...])
    """


def delta_merge(spark, df, options):
    source = COMMON_CONSTANTS.upper_Lower_random_string(15)
    df.createOrReplaceTempView(source)

    db_name = options.get("fact_db_name")
    table_name = options.get("fact_table_name")
    merge_condition = options.get(COMMON_CONSTANTS.MERGE_CONDITION)

    target = "{}.{}".format(db_name, table_name)
    merge_condition = "".join(merge_condition).format(target, source, target, source)

    do_update = options.get(COMMON_CONSTANTS.DO_UPDATE)
    do_delete = options.get(COMMON_CONSTANTS.DO_DELETE)
    do_insert = options.get(COMMON_CONSTANTS.DO_INSERT)

    print("Merge params " + str(options))

    merge_sql = " MERGE INTO {}.{} {}".format(db_name, table_name, "target")
    merge_sql = merge_sql + " USING {} {} ON {} ".format(source, "source", merge_condition)

    # column selection is pending
    if do_update is not None:
        update_condition = ""
        if COMMON_CONSTANTS.UPDATE_CONDITION in options.keys():
            update_condition = "AND " + options.get(COMMON_CONSTANTS.UPDATE_CONDITION)
        merge_sql = merge_sql + " WHEN MATCHED {} THEN UPDATE SET * ".format(update_condition)

    if do_delete is not None:
        delete_condition = ""
        if COMMON_CONSTANTS.DELETE_CONDITION in options.keys():
            delete_condition = "AND " + options.get(COMMON_CONSTANTS.DELETE_CONDITION)
        merge_sql = merge_sql + " WHEN MATCHED {} THEN DELETE ".format(delete_condition)

    # column selection is pending
    if do_insert is not None:
        merge_sql = merge_sql + " WHEN NOT MATCHED THEN INSERT * "

    print("SQL {} ".format(merge_sql))

    output = None

    output = spark.sql(merge_sql)

    spark.catalog.dropTempView(source)

    return (output)
