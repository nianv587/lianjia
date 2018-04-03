# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy
from scrapy.loader import ItemLoader
from scrapy.loader.processors import MapCompose, TakeFirst
from .settings import SQL_DATETIME_FORMAT, SQL_DATE_FORMAT


class Lianjia_itemloader(ItemLoader):
    default_output_processor = TakeFirst()

class LianjiaItem(scrapy.Item):

    title = scrapy.Field()#名称
    url = scrapy.Field()#链接
    city = scrapy.Field(
        input_processor=MapCompose(lambda x: x.replace("链家网", "").replace("站", ""))
    )#城市
    area = scrapy.Field(
        input_processor=MapCompose(lambda x: x.replace("二手房", ""))
    )#区域
    price = scrapy.Field()#总价
    unit_price = scrapy.Field()#均价
    community_name = scrapy.Field()#小区名称
    house_type = scrapy.Field()#房屋户型
    floor = scrapy.Field()#所在楼层
    floor_area = scrapy.Field()#建筑面积
    family_structure = scrapy.Field()#户型结构
    architectural_area = scrapy.Field()#套内面积
    architectural_style = scrapy.Field()#建筑类型
    orientations = scrapy.Field()#房屋朝向
    architectural_structure = scrapy.Field()#建筑结构
    decoration = scrapy.Field()#装修情况
    households =scrapy.Field()#梯户比例
    elevator = scrapy.Field()#配备电梯
    property = scrapy.Field()#产权年限
    feature = scrapy.Field(
        input_processor=MapCompose(lambda x: x.replace(" ", ""))
    )#房源特色
    listing_time = scrapy.Field()#挂牌时间
    crawl_time = scrapy.Field()#爬取时间

    def get_insert_sql(self):
        insert_sql = """
               insert into lianjia(title, url, city, area, price, unit_price, community_name, house_type,
               floor, floor_area, family_structure, architectural_area, architectural_style,orientations,
               architectural_structure, decoration, households, elevator, property, feature, listing_time, crawl_time) 
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
               ON DUPLICATE KEY UPDATE price=VALUES(price), crawl_time=VALUES(crawl_time)
           """
        params = (
            self["title"], self["url"], self["city"], self["area"], self["price"], self["unit_price"],
            self["community_name"], self["house_type"], self["floor"],
            self["floor_area"], self["family_structure"], self["architectural_area"],
            self["architectural_style"], self["orientations"], self["architectural_structure"],
            self["decoration"], self["households"], self["elevator"], self["property"],
            self["feature"], self["listing_time"], self["crawl_time"].strftime(SQL_DATETIME_FORMAT),
        )
        return insert_sql, params

