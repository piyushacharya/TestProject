from com.db.fw.etl.core.common.Constants import *
from pyspark.sql import SparkSession

def delta_insert(spark, df, options, mode ):
    db_name = options.get(COMMON_CONSTANTS.DB_NAME)
    table_name = options.get(COMMON_CONSTANTS.TABLE_NAME)

    input_options = options.get(COMMON_CONSTANTS.OPTIONS)

    df_writer = df.write \
        .format("delta") \
        .mode(mode)

    if COMMON_CONSTANTS.OPTIONS in options.keys():
        df_writer = df_writer.options(input_options)

    df_writer.save("{}.{}".format(db_name, table_name))






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


    update_table_name= COMMON_CONSTANTS.upper_Lower_random_string(15)
    df.createOrReplaceTempView(update_table_name)

    db_name = options.get(COMMON_CONSTANTS.DB_NAME)
    table_name = options.get(COMMON_CONSTANTS.TABLE_NAME)
    merge_condition = options.get(COMMON_CONSTANTS.MERGE_CONDITION)

    do_update = options.get(COMMON_CONSTANTS.DO_UPDATE)
    do_delete =  options.get(COMMON_CONSTANTS.DO_DELETE)
    do_insert = options.get(COMMON_CONSTANTS.DO_INSERT)


    merge_sql =" MERGE INTO {}.{} {}".format(db_name,table_name,"target")
    merge_sql = merge_sql.join( " USING {} {} ON ".format(update_table_name,"source" , merge_condition))

    # column selection is pending
    if do_update == "true":
        update_condition = ""
        if COMMON_CONSTANTS.UPDATE_CONDITION in options.keys():
            update_condition = "AND ".options.get(COMMON_CONSTANTS.UPDATE_CONDITION)
        merge_sql = merge_sql.join(" WHEN MATCHED {} THEN UPDATE * ".format(update_condition))


    if do_delete == "true":
        delete_condition = ""
        if COMMON_CONSTANTS.DELETE_CONDITION in options.keys():
            delete_condition = "AND ".options.get(COMMON_CONSTANTS.DELETE_CONDITION)
        merge_sql = merge_sql.join(" WHEN MATCHED {} THEN DELETE ".format(delete_condition))

    # column selection is pending
    if do_insert == "true":
        merge_sql = merge_sql.join(" WHEN NOT MATCHED THEN UPDATE * ")

    print ("SQL {} ".format(merge_sql))

    output= spark.sql(merge_sql)

    spark.catalog.dropTempView(update_table_name)

    return (output)



