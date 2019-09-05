
import mysql.connector
from mysql.connector import Error
from quotesbot.service.loginservice import get_logger

logger = get_logger(__name__)


class MysqlHandler:
    def __init__(self):
        self.connection = None
        self.connect()

    def connect(self):
        if not self.connection:
            try:
                self.connection = mysql.connector.connect(host='10.20.100.70',
                                                     database='ZCC_CUST',
                                                     user='root',
                                                     password='wensheng')
                if self.connection.is_connected():
                    db_Info = self.connection.get_server_info()
                    print("Connected to MySQL Server version ", db_Info)
                    cursor = self.connection.cursor()
                    cursor.execute("select database();")
                    record = cursor.fetchone()
                    print("Your connected to database: ", record)
            except Error as e:
                print("Error while connecting to MySQL", e)

    def putdata_regin_gov(self, id, name, parent_id):
        cursor = self.connection.cursor()
        record = (id, name, parent_id)
        mysql_select_query = """  SELECT id, name, parent_id  FROM CUST_REGION_DETAIL where id = %s"""
        mysql_delete_query = """  DELETE FROM  CUST_REGION_DETAIL where id = %s"""
        mysql_insert_query = """  INSERT INTO CUST_REGION_DETAIL (id, name, parent_id) VALUES( %s, %s, %s)"""
        mysql_update_query = """  UPDATE CUST_REGION_DETAIL SET  name = %s, parent_id = %s WHERE id = %s """
        cursor.execute(mysql_select_query, (id,))
        records = cursor.fetchall()
        if cursor.rowcount <= 0 :
            cursor.execute(mysql_insert_query, record)
            self.connection.commit()
        elif len(records) > 1:
            logger.error("there is more records for id:%s", id)
            cursor.execute(mysql_delete_query, (id,))
            cursor.execute(mysql_insert_query, record)
        else:
            if records[0][1] != name:
                logger.error("the name of DB:%s is not the name input:%s, will be updated", records[0].name, name)
                cursor.execute(mysql_update_query, (name, parent_id, id))
            else:
                logger.info("no update need for id:%s", id)





    def __del__(self):

        if self.connection is not None  and (self.connection.is_connected()):
            cursor = self.connection.cursor()
            cursor.close()
            self.connection.close()
            print("MySQL connection is closed")


    def query(self):
        cursor_orig = self.connection.cursor(buffered = True)
        cursor_gov = self.connection.cursor(buffered=True)
        region_select = """  SELECT id, name, parent_id  FROM CUST_REGION ORDER BY id ASC LIMIT %s, %s """
        region_gov_select = """  SELECT id, name, parent_id  FROM CUST_REGION_DETAIL ORDER BY id ASC LIMIT %s, %s """
        offset = 0
        pageSize = 20
        cursor_orig.execute(region_select, (offset, pageSize))
        records_region = cursor_orig.fetchall()
        while len(records_region) > 0:
            self.compare_gov(records_region)
            offset = offset + pageSize
            cursor_orig.execute(region_select, (offset, pageSize))
            records_region = cursor_orig.fetchall()
        offset = 0
        pageSize = 20

        cursor_gov.execute(region_gov_select, (offset, pageSize))
        records_gov = cursor_gov.fetchall()
        while len(records_gov) > 0:
            self.compare_origin(records_gov)
            offset = offset + pageSize
            cursor_gov.execute(region_gov_select, (offset, pageSize))
            records_gov = cursor_gov.fetchall()


    def compare_origin(self, records_gov):
        region_select = """  SELECT id, name, parent_id  FROM CUST_REGION WHERE id = %s """
        cursor_orig = self.connection.cursor(buffered=True)
        for record in records_gov:
            cursor_orig.execute(region_select, (record[0],))
            record_origin = cursor_orig.fetchall()
            if record_origin is None or len(record_origin) <= 0:
                print("官网记录 id:%s name:%s parent_id:%s 在原库中缺失"%( record[0], record[1], record[2] ))
            elif record_origin[0][1] != record[1]:
                print("官网记录 id:%s name:%s parent_id:%s 与原库中记录 id:%s name:%s parent_id:%s 不同"%(record[0], record[1],
                                            record[2], record_origin[0][0], record_origin[0][1], record_origin[0][2]))

    def compare_gov(self, records_orig):
        region_select = """  SELECT id, name, parent_id  FROM CUST_REGION_DETAIL WHERE id = %s """
        cursor_gov = self.connection.cursor(buffered=True)
        for record in records_orig:
            cursor_gov.execute(region_select, (record[0],))
            record_gov = cursor_gov.fetchall()
            if record_gov is None or len(record_gov) <= 0:
                print("原库记录 id:%s name:%s parent_id:%s 在官网中缺失" % (record[0], record[1], record[2]))
            elif record_gov[0][1] != record[1]:
                print("原库记录 id:%s name:%s parent_id:%s 与官网中记录 id:%s name:%s parent_id:%s 不同" % (record[0], record[1],
                    record[2], record_gov[0][0], record_gov[0][1], record_gov[0][2]))


if __name__ == "__main__":
    handler = MysqlHandler()
    handler.query()