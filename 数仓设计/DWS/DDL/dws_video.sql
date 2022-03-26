CREATE TABLE IF NOT EXISTS txkt.dws_video
  `video_id`             STRING COMMENT '视频ID',
  `video_title`          STRING COMMENT '视频标题',

  `add_study_num_1d`     BIGINT COMMENT '课程近一天增加学习人数',
  `add_study_num_3d`     BIGINT COMMENT '课程近三天增加学习人数',
  `add_study_num_10d`    BIGINT COMMENT '课程近十天增加学习人数',

  `add_person_num_1d`    BIGINT COMMENT '课程近一天增加购买/报名人数',
  `add_person_num_3d`    BIGINT COMMENT '课程近三天增加购买/报名人数',
  `add_person_num_10d`   BIGINT COMMENT '课程近十天增加购买/报名人数', 

  `profit_num_td`        BIGINT COMMENT '课程累计收益（分）',
  `profit_num_1d`        BIGINT COMMENT '课程近一天收益（分）',
  `profit_num_3d`        BIGINT COMMENT '课程近三天收益（分）',
  `profit_num_10d`       BIGINT COMMENT '课程近十天收益（分）', 

  `etl_time`             STRING COMMENT '数据加工时间'      
) 
PARTITIONED BY (`dt` STRING) 
STORED AS PARQUET