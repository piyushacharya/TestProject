
from com.db.fw.etl.core.writer.CommonWriters import BaseWriter
from pyspark.sql.functions import *
from pyspark.sql import *
from com.db.fw.etl.core.writer.CommonWritingUtils import *
from com.db.fw.etl.core.common.Constants import COMMON_CONSTANTS



class ForEachBatchWriter(BaseWriter):

    def get_latest_snapshot_rec(self,df,batch_id):
        df.createOrReplaceGlobalTempView("factView_{}".format(batch_id))
        latest_snaphot_df = self.spark.sql(
            "SELECT *, row_number() over (partition by customer_hash order by source_order_detail_updation_date desc) row_number,"
            "CASE WHEN (id % 2) = 0 THEN 'loyal' ELSE 'non-loyal' END customer_type FROM factView_${batchId} WHERE member_id_present = true AND cust_ hash_present = true ".format(batch_id))\
            .where("row_number = 1")\
            .drop("row_number")

        return latest_snaphot_df

    def process_row_postgres(self,row,curs_merge):
        val_1 = row.__getitem__("source_order_detail_id")
        val_2 = row.__getitem__("source_order_header_id")
        val_3 = row.__getitem__("customer_hash")
        val_4 = row.__getitem__("source_member_id")
        val_5 = row.__getitem__("source_member_address_id")
        val_6 = row.__getitem__("source_city_id")
        val_7 = row.__getitem__("source_hub_id")
        val_8 = row.__getitem__("source_slot_id")
        val_9 = row.__getitem__("source_sku_id")
        val_10 = row.__getitem__("supplier_id")
        val_11 = row.__getitem__("source_order_creation_date")
        val_12 = row.__getitem__("order_delivery_date")
        val_13 = row.__getitem__("order_number")
        val_14 = row.__getitem__("order_status")
        val_15 = row.__getitem__("order_channel")
        val_16 = row.__getitem__("fulfillment_type")
        val_17 = row.__getitem__("quantity")
        val_18 = row.__getitem__("unit_mrp")
        val_19 = row.__getitem__("unit_sale_price")
        val_20 = row.__getitem__("total_item_price")
        val_21 = row.__getitem__("source_order_detail_creation_date")
        val_22 = row.__getitem__("source_order_detail_updation_date")
        val_23 = row.__getitem__("order_type")
        val_24 = row.__getitem__("pick_location_id")
        val_25 = row.__getitem__("item_delivery_charge")
        val_26 = row.__getitem__("data_source")
        val_27 = row.__getitem__("source_sa_id")
        val_28 = row.__getitem__("source_delivery_mode_id")
        val_29 = row.__getitem__("loaddate")
        val_30 = row.__getitem__("partition_date")
        val_31 = row.__getitem__("added_date")
        val_32 = row.__getitem__("updated_date")
        val_33 = row.__getitem__("member_id_present")
        val_34 = row.__getitem__("cust_hash_present")
        sql_string = f"""INSERT INTO fact_table_tcp_payment \
        (source_order_detail_id,source_order_header_id,customer_hash,source_member_id,source_member_address_id,source_city_id,
        source_hub_id,source_slot_id,source_sku_id,supplier_id,source_order_creation_date,order_delivery_date,order_number,order_status,
        order_channel,fulfillment_type,quantity,unit_mrp,unit_sale_price,total_item_price,source_order_detail_creation_date,
        source_order_detail_updation_date,order_type,pick_location_id,item_delivery_charge,data_source,source_sa_id,source_delivery_mode_id,
        loaddate,partition_date ,added_date,updated_date,member_id_present,cust_hash_present)
      VALUES ('{val_1}', '{val_2}', '{val_3}','{val_4}', '{val_5}', '{val_6}','{val_7}', '{val_8}', '{val_9}', '{val_10}','{val_11}', '{val_12}', '{val_13}','{val_14}', '{val_15}', '{val_16}','{val_17}', '{val_18}', '{val_19}','{val_20}', '{val_21}', '{val_22}','{val_23}', '{val_24}', '{val_25}','{val_26}', '{val_27}', '{val_28}','{val_29}', '{val_30}', '{val_31}','{val_32}', '{val_33}', '{val_34}')
      ON CONFLICT (source_order_detail_id)
      DO
      UPDATE SET source_order_header_id=EXCLUDED.source_order_header_id,customer_hash=EXCLUDED.customer_hash,
      source_member_id=EXCLUDED.source_member_id,source_member_address_id=EXCLUDED.source_member_address_id,
      source_city_id=EXCLUDED.source_city_id,source_hub_id=EXCLUDED.source_hub_id,source_slot_id=EXCLUDED.source_slot_id,
      source_sku_id=EXCLUDED.source_sku_id,supplier_id=EXCLUDED.supplier_id,source_order_creation_date=EXCLUDED.source_order_creation_date,
      order_delivery_date=EXCLUDED.order_delivery_date,order_number=EXCLUDED.order_number,order_status=EXCLUDED.order_status,
      order_channel=EXCLUDED.order_channel,fulfillment_type=EXCLUDED.fulfillment_type,quantity=EXCLUDED.quantity,
      unit_mrp=EXCLUDED.unit_mrp,unit_sale_price=EXCLUDED.unit_sale_price,total_item_price=EXCLUDED.total_item_price,
      source_order_detail_creation_date=EXCLUDED.source_order_detail_creation_date,
      source_order_detail_updation_date=EXCLUDED.source_order_detail_updation_date,
      order_type=EXCLUDED.order_type,pick_location_id=EXCLUDED.pick_location_id,item_delivery_charge=EXCLUDED.item_delivery_charge,
      data_source=EXCLUDED.data_source,source_sa_id=EXCLUDED.source_sa_id,source_delivery_mode_id=EXCLUDED.source_delivery_mode_id,
      loaddate=EXCLUDED.loaddate,partition_date=EXCLUDED.partition_date,added_date=EXCLUDED.added_date,updated_date=EXCLUDED.updated_date,
      member_id_present=EXCLUDED.member_id_present,cust_hash_present=EXCLUDED.cust_hash_present"""
        curs_merge.execute(sql_string)

    def process_partition_postgres(self,partition):
        conn = psycopg2.connect(host=postgres_host,
                                user=postgres_user, password=postgres_pwd, database=postgres_database)
        dbc_merge = conn.cursor()
        for row in partition:
            self.process_row_postgres(row, dbc_merge)
        conn.commit()
        dbc_merge.close()
        conn.close()


    def upsert_to_postgres(self, df ):
        df_repart = df.repartition(32)
        df_repart.foreachPartition(self.process_partition_postgres)



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
        check_point = self.get_option_value("checkpointLocation")
        stream_input_df.writeStream\
            .foreachBatch(self.multi_location_write)\
            .outputMode("update") \
            .option("checkpointLocation", check_point)\
            .start()



