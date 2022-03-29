CREATE TABLE IF NOT EXISTS txkt.dws_video_type
(
  `video_type`                   STRING COMMENT '视频分类大类',
     
  `charge_video_num`             BIGINT COMMENT '收费视频数',
  `free_video_num`               BIGINT COMMENT '免费视频数',
      
  `expensive_video_price`        BIGINT COMMENT '最贵视频价格',
  `cheap_video_price`            BIGINT COMMENT '最便宜视频价格',
        
  `all_person_num`               BIGINT COMMENT '累计学习人数',
  `all_charge_person_num`        BIGINT COMMENT '视频购买总人数',
   
  `max_study_video`              STRING COMMENT '学习人数最多视频',
   
  `add_study_num_1d`             BIGINT COMMENT '近一天增加学习人数',
  `add_study_num_3d`             BIGINT COMMENT '近三天增加学习人数',
  `add_study_num_10d`            BIGINT COMMENT '近十天增加学习人数',
 
  `add_study_num_1d_max_video`   STRING COMMENT '近一天增加学习人数最多的视频ID',
  `add_study_num_3d_max_video`   STRING COMMENT '近三天增加学习人数最多的视频ID',
  `add_study_num_10d_max_video`  STRING COMMENT '近十天增加学习人数最多的视频ID',
 
  `add_person_num_1d`            BIGINT COMMENT '近一天增加购买/报名人数',
  `add_person_num_3d`            BIGINT COMMENT '近三天增加购买/报名人数',
  `add_person_num_10d`           BIGINT COMMENT '近十天增加购买/报名人数', 

  `add_person_num_1d_max_video`  STRING COMMENT '近一天增加购买/报名人数最多的的视频ID',
  `add_person_num_3d_max_video`  STRING COMMENT '近三天增加购买/报名人数最多的的视频ID',
  `add_person_num_10d_max_video` STRING COMMENT '近十天增加购买/报名人数最多的的视频ID', 

  `profit_num_td`                BIGINT COMMENT '累计收益（分）',
  `profit_num_1d`                BIGINT COMMENT '近一天收益（分）',
  `profit_num_3d`                BIGINT COMMENT '近三天收益（分）',
  `profit_num_10d`               BIGINT COMMENT '近十天收益（分）', 
        
  `etl_time`                     STRING COMMENT '数据加工时间'      
) 
PARTITIONED BY (`dt` STRING) 
STORED AS PARQUET