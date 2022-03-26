CREATE TABLE IF NOT EXISTS txkt.dws_organ
(
  `organ_name`                   STRING COMMENT '视频ID',
     
  `charge_video_num`             BIGINT COMMENT '机构收费视频数',
  `free_video_num`               BIGINT COMMENT '机构免费视频数',
      
  `expensive_video_price`        BIGINT COMMENT '机构最贵视频价格',
  `cheap_video_price`            BIGINT COMMENT '机构最便宜视频价格',
        
  `organ_all_person_num`         BIGINT COMMENT '机构累计学习人数',
  `organ_all_charge_person_num`  BIGINT COMMENT '机构视频购买总人数',
   
  `max_stduy_video`              STRING COMMENT '机构学习人数最多视频',
   
  `avg_page`                     BIGINT COMMENT '机构课程平均所在页数',
 
  `add_study_num_1d`             BIGINT COMMENT '机构近一天增加学习人数',
  `add_study_num_3d`             BIGINT COMMENT '机构近三天增加学习人数',
  `add_study_num_10d`            BIGINT COMMENT '机构近十天增加学习人数',
 
  `add_study_num_1d_max_video`   STRING COMMENT '机构近一天增加学习人数最多的视频ID',
  `add_study_num_3d_max_video`   STRING COMMENT '机构近三天增加学习人数最多的视频ID',
  `add_study_num_10d_max_video`  STRING COMMENT '机构近十天增加学习人数最多的视频ID',
 
  `add_person_num_1d`            BIGINT COMMENT '机构近一天增加购买/报名人数',
  `add_person_num_3d`            BIGINT COMMENT '机构近三天增加购买/报名人数',
  `add_person_num_10d`           BIGINT COMMENT '机构近十天增加购买/报名人数', 

  `add_person_num_1d_max_video`  STRING COMMENT '机构近一天增加购买/报名人数最多的的视频ID',
  `add_person_num_3d_max_video`  STRING COMMENT '机构近三天增加购买/报名人数最多的的视频ID',
  `add_person_num_10d_max_video` STRING COMMENT '机构近十天增加购买/报名人数最多的的视频ID', 

  `profit_num_td`                BIGINT COMMENT '机构累计收益（分）',
  `profit_num_1d`                BIGINT COMMENT '机构近一天收益（分）',
  `profit_num_3d`                BIGINT COMMENT '机构近三天收益（分）',
  `profit_num_10d`               BIGINT COMMENT '机构近十天收益（分）', 
        
  `etl_time`                     STRING COMMENT '数据加工时间'      
) 
PARTITIONED BY (`dt` STRING) 
STORED AS PARQUET