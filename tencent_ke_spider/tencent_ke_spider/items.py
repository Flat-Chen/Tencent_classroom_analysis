import scrapy


class TencentKeSpiderItem(scrapy.Item):
    # 视频大类
    video_type1 = scrapy.Field()
    # 视频中类
    video_type2 = scrapy.Field()
    # 视频小类
    video_type3 = scrapy.Field()
    # 视频标题
    video_title = scrapy.Field()
    # 视频url
    video_url = scrapy.Field()
    # 视频节数
    video_sections_num = scrapy.Field()
    # 视频所在页数
    video_index_page = scrapy.Field()
    # 机构名称
    organ_name = scrapy.Field()
    # 收费信息
    price = scrapy.Field()
    # 购买/报名人数
    person_num = scrapy.Field()
    # 最近在学
    recently_study_num = scrapy.Field()
    # 累计报名
    sign_up_num = scrapy.Field()
    # 好评度
    video_praise_degree = scrapy.Field()
    # 机构好评度
    organ_praise_degree = scrapy.Field()
    # 机构课程数
    organ_video_num = scrapy.Field()
    # 机构总学习人次
    organ_all_person_num = scrapy.Field()
    # 数据采集时间
    grab_time = scrapy.Field()



