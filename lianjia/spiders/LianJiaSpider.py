# -*- coding: utf-8 -*-
import scrapy
import json
import string
import re
from lianjia.items import Lianjia_itemloader, LianjiaItem
from datetime import datetime
from urllib import parse
from scrapy_redis.spiders import RedisSpider

class LianjiaspiderSpider(RedisSpider):
    redis_key = 'LianJiaSpider:start_urls'
    name = 'LianJiaSpider'
    # allowed_domains = ['lianjia.com']
    # start_urls = ['https://gz.lianjia.com/ershoufang/pg1']
    custom_settings = {
        "COOKIES_ENABLED": True,
        # "DOWNLOAD_DELAY": 0.5,
        # "RANDOMIZE_DOWNLOAD_DELAY": True,
        "DEFAULT_REQUEST_HEADERS": {
            'Accept': 'application/json, text/javascript, */*; q=0.01',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'zh-CN,zh;q=0.8',
            'Connection': 'keep-alive',
            'Cookie': 'lianjia_uuid=b980dda7-0c61-42d6-990a-a0001c53cc3b; _smt_uid=5a9943fc.57426799; UM_distinctid=161e6b193554e8-0ab725fbcffa83-3e3d5100-13c680-161e6b19356987; _ga=GA1.2.1036933437.1519993854; _gid=GA1.2.309047760.1519993854; gr_user_id=5455f0bb-468d-49fd-8ab2-f29737680339; _jzqx=1.1520007339.1520338148.3.jzqsr=gz%2Elianjia%2Ecom|jzqct=/ershoufang/.jzqsr=gz%2Elianjia%2Ecom|jzqct=/ershoufang/; select_city=440100; _jzqckmp=1; sample_traffic_test=0; lianjia_ssid=15f25b50-b1b5-4f61-ac38-8c0c8237518f; all-lj=406fadba61ceb7b8160b979dadec8dfa; Hm_lvt_9152f8221cb6243a53c83b956842be8a=1520308543,1520389986,1520482344,1520567884; _qzjc=1; CNZZDATA1255849599=1589111258-1519636574-https%253A%252F%252Fwww.lianjia.com%252F%7C1520566602; _jzqa=1.4029491555921870300.1519899332.1520484846.1520567884.27; _jzqc=1; CNZZDATA1254525948=1584036093-1519637204-https%253A%252F%252Fwww.lianjia.com%252F%7C1520564000; CNZZDATA1255633284=608189627-1519635836-https%253A%252F%252Fwww.lianjia.com%252F%7C1520566843; CNZZDATA1255604082=1505380257-1519639370-https%253A%252F%252Fwww.lianjia.com%252F%7C1520563051; Hm_lpvt_9152f8221cb6243a53c83b956842be8a=1520567885; _qzja=1.1866512804.1519640810964.1520484846065.1520567883700.1520567883700.1520567885391.0.0.0.399.41; _qzjb=1.1520567883700.2.0.0.0; _qzjto=2.1.0; _jzqb=1.2.10.1520567884.1; _gat=1; _gat_global=1; _gat_new_global=1; _gat_dianpu_agent=1',
            # 'Cookie': 'lianjia_uuid=f9bd3c1c-dcf2-44ea-b24a-7c3d8990b355; _jzqy=1.1519640805.1519640805.1.jzqsr=baidu.-; _jzqckmp=1; UM_distinctid=161d1a67cd635e-05735a081fb9c3-3e3d5100-13c680-161d1a67cd7824; _ga=GA1.2.1723090428.1519640806; _gid=GA1.2.1936429781.1519640806; select_city=440100; _smt_uid=5a93e0eb.4e47e81a; gr_user_id=a480685b-9dc9-45a5-ae2f-10c31f7b5de6; all-lj=eae2e4b99b3cdec6662e8d55df89179a; Hm_lvt_9152f8221cb6243a53c83b956842be8a=1519640811,1519643487; _qzjc=1; _jzqc=1; lianjia_ssid=eaf4c357-efb9-44fe-8390-f605a0f6ce3e; _jzqa=1.4354183199935802000.1519640805.1519648685.1519656635.4; _jzqx=1.1519643487.1519656635.3.jzqsr=lianjia%2Ecom|jzqct=/.jzqsr=gz%2Elianjia%2Ecom|jzqct=/ershoufang/co32/; CNZZDATA1255849599=1589111258-1519636574-https%253A%252F%252Fwww.lianjia.com%252F%7C1519652778; CNZZDATA1254525948=1584036093-1519637204-https%253A%252F%252Fwww.lianjia.com%252F%7C1519653405; CNZZDATA1255633284=608189627-1519635836-https%253A%252F%252Fwww.lianjia.com%252F%7C1519652081; CNZZDATA1255604082=1505380257-1519639370-https%253A%252F%252Fwww.lianjia.com%252F%7C1519653791; _gat=1; _gat_global=1; _gat_new_global=1; _gat_dianpu_agent=1; Hm_lpvt_9152f8221cb6243a53c83b956842be8a=1519657058; _qzja=1.1866512804.1519640810964.1519648685049.1519656634751.1519657055718.1519657058482.0.0.0.18.4; _qzjb=1.1519656634751.7.0.0.0; _qzjto=18.4.0; _jzqb=1.7.10.1519656635.1',
            'Host': 'gz.lianjia.com',
            # 'Origin': 'https://www.lagou.com',
            # 'Referer': 'https://www.lagou.com/',
            'Upgrade - Insecure - Requests': '1',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36',
        }
    }

    def start_requests(self):
        yield scrapy.Request(url='https://gz.lianjia.com/ershoufang/', callback=self.parse_district)
        # yield scrapy.Request(url='https://gz.lianjia.com/ershoufang/GZ0003117147.html', callback=self.parse_detail)

    def parse_district(self, response):
        district_nodes = response.xpath('//div[@data-role="ershoufang"]/div[1]/a')
        for i in district_nodes:
            district_url = i.xpath('@href').extract_first()
            yield scrapy.Request(url=parse.urljoin(response.url, district_url), callback=self.parse_area)

    def parse_area(self, response):
        area_nodes = response.xpath('//div[@class="list-more"]/dl[2]/dd/a')
        for i in area_nodes:
            area_url = i.xpath('@href').extract_first()
            yield scrapy.Request(url=parse.urljoin(response.url, area_url), callback=self.parse_list)

    def parse_list(self, response):
        list_nodes = response.xpath('//ul[contains(@class,"sellListContent")]/li/a')
        a = open("C:/Users/nian/lianjia/count.txt", "a")
        a.write(response.url + '   ' + str(len(list_nodes)) + '\n')
        a.close()
        for list_node in list_nodes:
            list_url = list_node.xpath("@href").extract_first()
            f = open("C:/Users/nian/lianjia/url.txt", "a")
            f.write(response.url+'   '+list_url+'\n')
            f.close()
            yield scrapy.Request(url=list_url, callback=self.parse_detail)
        try:
            totalPage_re = re.match('.*{"totalPage":(.*?),"curPage":(.*?)}', response.text, re.DOTALL)
            totalPage = int(totalPage_re.group(1))
            curPage = int(totalPage_re.group(2))
            if curPage != totalPage:
                curPage += 1
                next_urls = re.match('https://gz.lianjia.com/ershoufang/(.*?)/(.*?)/(.*?)/', response.url,
                                     re.DOTALL)  # 'https://gz.lianjia.com/ershoufang/tianhe/a1/'
                if not next_urls:
                    url = response.url + 'pg' + str(curPage)
                    yield scrapy.Request(url=url, callback=self.parse_list)
                else:
                    next_url = response.url.rstrip('/').rstrip(string.digits)
                    url = next_url + str(curPage)
                    yield scrapy.Request(url=url, callback=self.parse_list)
        except Exception as e:
            print(e)
            print(response.url)
            yield scrapy.Request(url=response.url, callback=self.parse_list, dont_filter=True)

    def parse_detail(self, response):
        item_loader = Lianjia_itemloader(item=LianjiaItem(), response=response)
        item_loader.add_xpath("title", "//div[@class='title-wrapper']/div[@class='content']/div[@class='title']/h1/text()")
        item_loader.add_value("url", response.url)
        item_loader.add_xpath("city", "//div[@class='fl l-txt']/a[1]/text()")
        area = response.xpath("//div[@class='fl l-txt']/a[3]/text()").extract_first("暂无数据")
        if area =='暂无数据'or not area:
            print(area)
            print(response.text)
            print(response.url)
        item_loader.add_value("area", area)
        item_loader.add_xpath("price", "//span[@class='total']/text()")
        item_loader.add_xpath("unit_price", "//span[@class='unitPriceValue']/text()")
        item_loader.add_xpath("community_name", "//div[@class='communityName']/a[@target='_blank']/text()")
        house_type = response.xpath("//span[contains(text(), '房屋户型')]/../text()").extract_first("暂无数据")
        floor = response.xpath("//span[contains(text(), '所在楼层')]/../text()").extract_first("暂无数据")
        floor_area = response.xpath("//span[contains(text(), '建筑面积')]/../text()").extract_first("暂无数据")
        family_structure = response.xpath("//span[contains(text(), '户型结构')]/../text()").extract_first("暂无数据")
        architectural_area = response.xpath("//span[contains(text(), '套内面积')]/../text()").extract_first("暂无数据")
        architectural_style = response.xpath("//span[contains(text(), '建筑类型')]/../text()").extract_first("暂无数据")
        orientations = response.xpath("//span[contains(text(), '房屋朝向')]/../text()").extract_first("暂无数据")
        architectural_structure = response.xpath("//span[contains(text(), '建筑结构')]/../text()").extract_first("暂无数据")
        decoration = response.xpath("//span[contains(text(), '装修情况')]/../text()").extract_first("暂无数据")
        households = response.xpath("//span[contains(text(), '梯户比例')]/../text()").extract_first("暂无数据")
        elevator = response.xpath("//span[contains(text(), '配备电梯')]/../text()").extract_first("暂无数据")
        property = response.xpath("//span[contains(text(), '产权年限')]/../text()").extract_first("暂无数据")
        item_loader.add_value("house_type", house_type)
        item_loader.add_value("floor", floor)
        item_loader.add_value("floor_area", floor_area)
        item_loader.add_value("family_structure", family_structure)
        item_loader.add_value("architectural_area", architectural_area)
        item_loader.add_value("architectural_style", architectural_style)
        item_loader.add_value("orientations", orientations)
        item_loader.add_value("architectural_structure", architectural_structure)
        item_loader.add_value("decoration", decoration)
        item_loader.add_value("households", households)
        item_loader.add_value("elevator", elevator)
        item_loader.add_value("property", property)
        item_loader.add_xpath("listing_time", "//div[@class='transaction']/div[@class='content']/ul/li[1]/span[2]/text()")
        item_loader.add_value("crawl_time", datetime.now())
        feature_list = response.xpath("//div[@class='baseattribute clear']//div[@class='content']//text()").extract()
        feature = "".join([each for each in feature_list])
        if not feature:
            item_loader.add_value("feature", '暂无数据')
        else:
            item_loader.add_value("feature", feature)
        lianjia_item = item_loader.load_item()
        yield lianjia_item



