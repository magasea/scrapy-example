# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
from scrapy.exporters import JsonItemExporter
from quotesbot.service.mysqlservice import MysqlHandler

class QuotesbotPipeline(object):
    def process_item(self, item, spider):
        return item
sqlHandler = MysqlHandler()

class JsonPipeline(object):
    def __init__(self):
        self.file = open("gov.json", 'wb')
        self.exporter = JsonItemExporter(self.file, encoding='utf-8', ensure_ascii=False)
        self.exporter.start_exporting()


    def close_spider(self, spider):
        self.exporter.finish_exporting()
        self.file.close()

    def process_item(self, item, spider):
        self.exporter.export_item(item)
        for key_str in item:
            if "second" in key_str:
                print("begin to handle %s"%key_str)
            else:
                continue
            keys = key_str.split(":")
            province_code = keys[1].replace(".html", "0000")
            province_name = keys[2]
            parent_id = 0
            sqlHandler.putdata_regin_gov(province_code, province_name, parent_id)
            city_code = keys[4] if keys[4].isdigit() else keys[5]
            city_name = keys[5] if keys[4].isdigit() else keys[4]
            city_code = city_code[:6]
            city_parent_id = province_code
            sqlHandler.putdata_regin_gov(city_code, city_name, city_parent_id)

            subItem = item[key_str]
            for sub_key_str in subItem:
                county = subItem[sub_key_str]
                county_code = county[0] if county[0].isdigit() else county[1]
                county_name = county[1] if county[0].isdigit() else county[0]
                county_code = county_code[:6]
                county_parent_id = city_code
                sqlHandler.putdata_regin_gov(county_code, county_name, county_parent_id)

        return item


class DetailPipeline(object):
    def __init__(self):
        self.file = open("gov.json", 'wb')
        self.exporter = JsonItemExporter(self.file, encoding='utf-8', ensure_ascii=False)
        self.exporter.start_exporting()


    def close_spider(self, spider):
        self.exporter.finish_exporting()
        self.file.close()

    def process_item(self, item, spider):
        self.exporter.export_item(item)
        for key_str in item:
            if "third" in key_str:
                print("begin to handle %s"%key_str)
            else:
                continue
            keys = key_str.split(":")
            province_code = keys[1].replace(".html", "0000000000")
            province_name = keys[2]
            parent_id = 0
            sqlHandler.putdata_regin_gov(province_code, province_name, parent_id)
            city_code = keys[4] if keys[4].isdigit() else keys[5]
            city_name = keys[5] if keys[4].isdigit() else keys[4]
            city_parent_id = province_code
            sqlHandler.putdata_regin_gov(city_code, city_name, city_parent_id)
            county_code = keys[7] if keys[7].isdigit() else keys[8]
            county_name = keys[8] if keys[7].isdigit() else keys[7]
            county_parent_id = city_code
            sqlHandler.putdata_regin_gov(county_code, county_name, county_parent_id)


            subItem = item[key_str]
            for sub_key_str in subItem:
                town = subItem[sub_key_str]
                town_code = town[0] if town[0].isdigit() else town[1]
                town_name = town[1] if town[0].isdigit() else town[0]
                town_parent_id = county_code
                sqlHandler.putdata_regin_gov(town_code, town_name, town_parent_id)

        return item