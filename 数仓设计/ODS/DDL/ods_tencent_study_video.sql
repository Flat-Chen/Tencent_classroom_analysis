CREATE TABLE IF NOT EXISTS txkt.ods_tencent_study_video
 (
  `video_type1`          STRING COMMENT '视频大类',
  `video_type2`          STRING COMMENT '视频中类',
  `video_type3`          STRING COMMENT '视频小类',
  `video_title`          STRING COMMENT '视频标题',
  `video_url`            STRING COMMENT '视频url',
  `video_sections_num`   STRING COMMENT '视频节数',
  `video_index_page`     INT    COMMENT '视频所在页数',
  `organ_name`           STRING COMMENT '机构名称',
  `price`                STRING COMMENT '收费信息(单位：分)',
  `person_num`           STRING COMMENT '购买/报名人数',
  `recently_study_num`   STRING COMMENT '最近在学',
  `sign_up_num`          STRING COMMENT '累计报名',
  `video_praise_degree`  STRING COMMENT '好评度',
  `organ_praise_degree`  STRING COMMENT '机构好评度',
  `organ_video_num`      STRING COMMENT '机构课程数',
  `organ_all_person_num` STRING COMMENT '机构总学习人次',
  `grab_time`            STRING COMMENT '数据采集时间',
  `etl_time`             STRING COMMENT '数据加工时间'
) 
PARTITIONED BY (`dt` STRING) 
STORED AS PARQUET