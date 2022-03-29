CREATE TABLE IF NOT EXISTS txkt.ads_video
(
  `video_id`             STRING COMMENT '视频ID',
  `video_title`          STRING COMMENT '视频标题',
  `add_study_num_10d`    BIGINT COMMENT '课程近十天增加学习人数',
  `add_person_num_10d`   BIGINT COMMENT '课程近十天增加购买/报名人数' 
) 
STORED AS PARQUET
