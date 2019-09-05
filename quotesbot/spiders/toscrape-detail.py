# -*- coding: utf-8 -*-
import json

import scrapy

from quotesbot.service.loginservice import get_logger

logger = get_logger(__name__)
class GovSpider(scrapy.Spider):

    name = "toscrape-detail"
    start_urls = [
        'http://www.stats.gov.cn/tjsj/tjbz/tjyqhdmhcxhfdm/2018/index.html',
    ]

    provinces_urls = []

    def parse(self, response):

        records = {}
        for quote in response.xpath("//tr[@class='provincetr']/td"):
            province_ref = quote.css("a::attr(href)").extract_first()
            content_ref = quote.css("a::text").extract_first()




            if province_ref is None and content_ref is None:
                continue
            if province_ref in records:
                records[province_ref].append(content_ref)
            else:
                records[province_ref] = {}
                records[province_ref] = [ content_ref if content_ref is not None and content_ref.isdigit() else content_ref]

            logger.info('parse: %s, %s',province_ref, content_ref)
        for province_ref in records:
            yield scrapy.Request('http://www.stats.gov.cn/tjsj/tjbz/tjyqhdmhcxhfdm/2018/%s' % province_ref,
                                 callback=lambda response, parent='top:%s:%s'%(province_ref, records[province_ref][0]): self.parseProvince(response, parent))

        yield records
        jsonOutput = json.dumps(records, ensure_ascii=False)
        logger.info(jsonOutput)
        records = {}

    def parseProvince(self, response, parent):
        records = {}
        total = {}
        for quote in response.xpath("//tr[@class='citytr']/td"):
            city_ref = quote.css("a::attr(href)").extract_first()
            content_ref = quote.css("a::text").extract_first()

            if city_ref is None and content_ref is None:
                continue
            if city_ref in records:
                records[city_ref].append(content_ref)
                yield scrapy.Request('http://www.stats.gov.cn/tjsj/tjbz/tjyqhdmhcxhfdm/2018/%s' % city_ref,
                                     callback= lambda response, parent='%s:second:%s:%s'%(parent, records[city_ref][0], records[city_ref][1]): self.parseCity(response, parent))
            else:
                records[city_ref] = {}
                records[city_ref] = [content_ref if content_ref is not None and content_ref.isdigit() else content_ref]

        total[parent] = records
        jsonOutput = json.dumps(total, ensure_ascii=False)
        logger.info(jsonOutput)
        records = {}
        yield total

    def parseCity(self, response, parent):
        total = {}
        records = {}
        for quote in response.xpath("//tr[@class='countytr']/td"):
            county_ref = quote.css("a::attr(href)").extract_first()
            content_ref = quote.css("a::text").extract_first()
            if county_ref is None and content_ref is None:
                continue
            if county_ref in records:
                records[county_ref].append(content_ref)
                url = response.url[:response.url.rfind('/')+1]
                yield scrapy.Request(url + county_ref, callback=lambda response, parent='%s:third:%s:%s' % (
                                     parent, records[county_ref][0], records[county_ref][1]): self.parseCounty(response,
                                                                                                         parent))
            else:
                records[county_ref] = {}
                records[county_ref] = [content_ref if content_ref is not None and content_ref.isdigit() else content_ref]
            # yield {
            #     'text': quote.css("a::text").extract_first(),
            #     'href': quote.css("a::attr(href)").extract_first()
            # }
            if quote.css("a::attr(href)").extract_first() is not None:
                pass
                # yield scrapy.Request('http://www.stats.gov.cn/tjsj/tjbz/tjyqhdmhcxhfdm/2018/%s' % quote.css("a::attr(href)").extract_first(), self.parseCounty)
        total[parent] = records
        jsonOutput = json.dumps(total, ensure_ascii=False)
        logger.info(jsonOutput)
        records = {}
        yield total


    def parseCounty(self, response, parent):
        total = {}
        records = {}
        for quote in response.xpath("//tr[@class='towntr']/td"):
            town_ref = quote.css("a::attr(href)").extract_first()
            content_ref = quote.css("a::text").extract_first()
            if town_ref is None and content_ref is None:
                continue
            if town_ref in records:
                print("URL: " + response.request.url)
                records[town_ref].append(content_ref)
                # yield scrapy.Request('http://www.stats.gov.cn/tjsj/tjbz/tjyqhdmhcxhfdm/2018/%s' % town_ref,
                #                      callback=lambda response, parent='%s:fourth:%s:%s' % (
                #                      parent, records[town_ref][0], records[town_ref][1]): self.parseCounty(response,
                #                                                                                          parent))
            else:
                records[town_ref] = {}
                records[town_ref] = [content_ref if content_ref is not None and content_ref.isdigit() else content_ref]
            # yield {
            #     'text': quote.css("a::text").extract_first(),
            #     'href': quote.css("a::attr(href)").extract_first()
            # }
            if quote.css("a::attr(href)").extract_first() is not None:
                pass
                # yield scrapy.Request('http://www.stats.gov.cn/tjsj/tjbz/tjyqhdmhcxhfdm/2018/%s' % quote.css("a::attr(href)").extract_first(), self.parseCounty)
        total[parent] = records
        jsonOutput = json.dumps(total, ensure_ascii=False)
        logger.info(jsonOutput)
        records = {}
        yield total
