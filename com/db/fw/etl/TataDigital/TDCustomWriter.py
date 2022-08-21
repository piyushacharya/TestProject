import json

from com.db.fw.etl.TataDigital.TDUtilities import PostgresWriterService
from com.db.fw.etl.core.writer.CommonWriters import BaseWriter
from pyspark.sql.functions import *
from pyspark.sql import *
from com.db.fw.etl.core.writer.CommonWritingUtils import *
from com.db.fw.etl.core.common.Constants import COMMON_CONSTANTS
import psycopg2


# This class will be replced by ForEachBatchWriterNotebook
class ForEachBatchWriter(BaseWriter):

    def get_latest_snapshot_rec(self,df,batch_id):
        df.createOrReplaceGlobalTempView("factView_{}".format(batch_id))
        latest_snaphot_df = self.spark.sql(
            "SELECT *, row_number() over (partition by customer_hash order by source_order_detail_updation_date desc) row_number,"
            "CASE WHEN (id % 2) = 0 THEN 'loyal' ELSE 'non-loyal' END customer_type FROM factView_${batchId} WHERE member_id_present = true AND cust_ hash_present = true ".format(batch_id))\
            .where("row_number = 1")\
            .drop("row_number")
        return latest_snaphot_df


    def write_to_history(self, df ):
        insert_options = {}
        insert_options[COMMON_CONSTANTS.DB_NAME] = self.get_option_value("history_db_name")
        insert_options[COMMON_CONSTANTS.TABLE_NAME] = self.get_option_value("history_table_name")

        delta_insert(df,insert_options,COMMON_CONSTANTS.APPEND)

    def merge_to_fact(self,df,batch_id):
        merge_options = {}
        merge_options[COMMON_CONSTANTS.DB_NAME] = self.get_option_value("fact_db_name")
        merge_options[COMMON_CONSTANTS.TABLE_NAME] = self.get_option_value("fact_table_name")
        merge_options[COMMON_CONSTANTS.DO_UPDATE] = "true"
        merge_options[COMMON_CONSTANTS.DO_INSERT] = "true"
        merge_options[COMMON_CONSTANTS.MERGE_CONDITION] = " target.customer_hash = source.customer_hash AND target.source_order_detail_creation_date = source.source_order_detail_creation_date "
        merge_options[COMMON_CONSTANTS.UPDATE_CONDITION] = "target.source_order_detail_updation_date < source.source_order_detail_updation_date"
        delta_merge(df,merge_options)



    def multi_location_write(self,df, batch_id):


        # Run below code in thread

        # Write to History
        self.writeToHistory(df)

        #write to Exception Table


        # Write to History
        latest_snapshot_df = self.getLatestSnapshotRec(df,batch_id)
        self.merge_to_fact(latest_snapshot_df,batch_id)

        # Write to Postgres
        self.upsert_to_postgres(latest_snapshot_df)

    def execute(self):
        stream_input_df = self.get_input_dataframe();
        param_json = json.dumps(self.input_options);

        stream_input_df = stream_input_df.withColumn("param",lit(param_json))
        check_point = self.get_option_value("checkpointLocation")

        stream_input_df.writeStream\
            .foreachBatch(self.multi_location_write)\
            .outputMode("update") \
            .option("checkpointLocation", check_point)\
            .start()


class ForEachBatchWriterNotebook(BaseWriter):

    def get_latest_snapshot_rec(self, df, batch_id):
        df.createOrReplaceTempView("factView".format(batch_id))
        latest_snaphot_df = self.spark.sql(
            f"""SELECT*,row_number() over (partition by customer_hash order by source_order_detail_updation_date desc) row_number, CASE WHEN (payOrderId % 2) = 0 THEN 'loyal' ELSE 'non-loyal' END customer_type FROM factView WHERE member_id_present = true AND cust_hash_present = true """).where(
            "row_number = 1").drop("row_number")

        return latest_snaphot_df

    def multi_location_write(self, df, batch_id):
       pass

    def execute(self):
        history_df = self.get_input_dataframe();

        check_point = self.get_option_value("checkpointLocation")


        batch_id = None

        part_count = None,
        postgres_host = None,
        postgres_user = None,
        postgres_pwd = None,
        postgres_database = None

        #         history_df = history_df.distinct()

        #         history_df = history_df.limit(1000)
        print("History count {}".format(history_df.count()))
        pgWriter = PostgresWriterService()
        pgWriter.upsert(history_df)


        self.set_output_dataframe(history_df)
