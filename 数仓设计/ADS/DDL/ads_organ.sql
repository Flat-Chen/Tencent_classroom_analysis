CREATE TABLE IF NOT EXISTS txkt.ads_organ
(
  `organ_name`                   STRING COMMENT '机构名称',
  `max_study_video`              STRING COMMENT '机构学习人数最多视频',
  `avg_page`                     BIGINT COMMENT '机构课程平均所在页数',
  `add_person_num_10d`           BIGINT COMMENT '机构近十天增加购买/报名人数', 
  `add_person_num_10d_max_video` STRING COMMENT '机构近十天增加购买/报名人数最多的的视频ID'
) 
STORED AS PARQUET
