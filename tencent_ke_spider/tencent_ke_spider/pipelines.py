import logging
import pymysql
import pandas as pd
from sqlalchemy import create_engine
import time

today = time.strftime("%Y_%m_%d", time.localtime())


class TencentKeSpiderPipeline:
    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler.settings)

    def __init__(self, settings):
        self.conn = create_engine('mysql+pymysql://txkt:jKmnGWj6szmDa8hw@101.35.21.127:3306/txkt?charset=utf8mb4')
        self.mysqlcounts = 0
        ddl = '''
CREATE TABLE IF NOT EXISTS tencent_study_video_spider_{}
(
    id                   INT UNSIGNED AUTO_INCREMENT COMMENT '自增主键',
    video_type1          VARCHAR(64) COMMENT '视频大类',
    video_type2          VARCHAR(64) COMMENT '视频中类',
    video_type3          VARCHAR(64) COMMENT '视频小类',
    video_title          VARCHAR(64) COMMENT '视频标题',
    video_url            VARCHAR(64) COMMENT '视频url',
    video_sections_num   VARCHAR(64) COMMENT '视频节数',
    video_index_page     INT         COMMENT '视频所在页数',
    organ_name           VARCHAR(64) COMMENT '机构名称',
    price                VARCHAR(64) COMMENT '收费信息(单位：分)',
    person_num           VARCHAR(64) COMMENT '购买/报名人数',
    recently_study_num   VARCHAR(64) COMMENT '最近在学',
    sign_up_num          VARCHAR(64) COMMENT '累计报名',
    video_praise_degree  VARCHAR(64) COMMENT '好评度',
    organ_praise_degree  VARCHAR(64) COMMENT '机构好评度',
    organ_video_num      VARCHAR(64) COMMENT '机构课程数',
    organ_all_person_num VARCHAR(64) COMMENT '机构总学习人次',
    grab_time            VARCHAR(64) COMMENT '数据采集时间',
    PRIMARY KEY (id)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8;
'''.format(today)
        db = pymysql.connect(
            host='101.35.21.127',
            user='txkt',
            password='jKmnGWj6szmDa8hw',
            database='txkt'
        )
        cursor = db.cursor()
        cursor.execute(ddl)
        cursor.fetchall()
        db.commit()
        db.close()

    def process_item(self, item, spider):
        self.mysqlcounts += 1
        logging.log(msg=f"scrapy              {self.mysqlcounts}              items", level=logging.INFO)
        # 数据存入mysql
        items = list()
        items.append(item)
        df = pd.DataFrame(items)
        df.to_sql(name='tencent_study_video_spider_{}'.format(today), con=self.conn, if_exists="append", index=False)
        logging.log(msg=f"add data in mysql", level=logging.INFO)

    def close_spider(self, spider):
        self.conn.close()
