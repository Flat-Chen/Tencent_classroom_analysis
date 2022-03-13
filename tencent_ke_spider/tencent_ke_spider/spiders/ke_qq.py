import json
import re
import time
import scrapy
from tencent_ke_spider.items import TencentKeSpiderItem


class KeQqSpider(scrapy.Spider):
    name = 'ke_qq'
    allowed_domains = ['ke.qq.com']
    start_urls = ['https://ke.qq.com/index.html']

    def parse(self, response, *args, **kwargs):
        video_index_page = 1
        class_types = response.xpath('//a[@class="mod-nav__link-nav-third mod-nav__wrap-nav-third_line"]')
        for class_type in class_types:
            class_type_url = class_type.xpath('./@href').extract_first()
            yield scrapy.Request(
                url=response.urljoin(class_type_url),
                callback=self.get_all_videos,
                meta={'video_index_page': video_index_page}
            )

    def get_all_videos(self, response):
        video_index_page = response.meta['video_index_page']
        # 翻页
        next_page_url = response.xpath('//a[@class="page-next-btn icon-font i-v-right"]/@href').extract_first()
        if next_page_url:
            video_index_page = video_index_page + 1
            yield scrapy.Request(
                url=response.urljoin(next_page_url),
                callback=self.get_all_videos,
                meta={'video_index_page': video_index_page}
            )
        data_info = re.findall(r'localData = (.*?)</script>', response.text, re.S)[0].strip()[:-1]
        data_json = json.loads(data_info)
        video_info_dict = {}
        for i in data_json['items']:
            try:
                person_num = i['recent_sign_num']
            except:
                person_num = i['apply_num']
            video_info_dict[str(i['cid'])] = {
                'video_sections_num': i['total_task_num'],
                'organ_name': i['agency_name'],
                'price': i['price'],
                'person_num': person_num
            }

        videos = response.xpath('//li[@class="course-card-item--v3 js-course-card-item "]')
        for i in videos:
            video_url = i.xpath('./a/@href').extract_first()
            video_cid = i.xpath('./a/@data-id').extract_first()
            video_sections_num = video_info_dict[video_cid]['video_sections_num']
            organ_name = video_info_dict[video_cid]['organ_name']
            price = video_info_dict[video_cid]['price']
            person_num = video_info_dict[video_cid]['person_num']

            yield scrapy.Request(
                url=response.urljoin(video_url),
                callback=self.get_video_info,
                meta={
                    'video_index_page': video_index_page,
                    'video_sections_num': video_sections_num,
                    'organ_name': organ_name,
                    'price': price,
                    'person_num': person_num
                }
            )

    def get_video_info(self, response):
        item = TencentKeSpiderItem()
        item['video_index_page'] = response.meta['video_index_page']
        item['video_sections_num'] = response.meta['video_sections_num']
        item['organ_name'] = response.meta['organ_name']
        item['price'] = response.meta['price']
        item['person_num'] = response.meta['person_num']
        item['video_url'] = response.url

        video_types = response.xpath('//nav[@class="breadcrumb inner-center"]/a/text()').getall()
        item['video_type1'] = video_types[1].strip()
        item['video_type2'] = video_types[2].strip()
        item['video_type3'] = video_types[3].strip()
        item['video_title'] = video_types[4].strip()
        if response.meta['price'] == 0:
            video_info = response.xpath('//div[@class="course-hints"]/span[@class="line-item"]')
            item['recently_study_num'] = video_info[0].xpath('./span[@class="hint-data"]/text()') \
                .extract_first().strip()
            item['sign_up_num'] = video_info[1].xpath('./span[@class="hint-data"]/text()') \
                .extract_first().strip()
            item['video_praise_degree'] = video_info[2].xpath('./span[@class="hint-data"]/text()') \
                .extract_first().strip()
        else:
            item['video_praise_degree'] = response.xpath('//span[@class="line-item statistics-rate"]/span/text()') \
                .extract_first().strip()
        organ = response.xpath('//ul[@class="tree-list"]/li')
        item['organ_praise_degree'] = organ[0].xpath('./span/text()').extract_first().strip()
        item['organ_video_num'] = organ[1].xpath('./span/@data-num').extract_first().strip()
        item['organ_all_person_num'] = organ[2].xpath('./span/@data-num').extract_first().strip()
        item['grab_time'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        # yield item
        print(item)
