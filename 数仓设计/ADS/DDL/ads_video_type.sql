CREATE TABLE IF NOT EXISTS txkt.ads_video_type
(
  `video_type`                   STRING COMMENT '视频分类大类',    
  `max_study_video`              STRING COMMENT '学习人数最多视频',
  `add_study_num_10d`            BIGINT COMMENT '近十天增加学习人数',
  `add_person_num_10d`           BIGINT COMMENT '近十天增加购买/报名人数', 
  `add_person_num_10d_max_video` STRING COMMENT '近十天增加购买/报名人数最多的的视频ID'     
) 
STORED AS PARQUET

