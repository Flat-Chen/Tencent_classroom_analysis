CREATE TABLE IF NOT EXISTS txkt.dim_video_df
 (
  `video_id`             STRING COMMENT '视频ID',
  `video_title`          STRING COMMENT '视频标题',
  `video_type1`          STRING COMMENT '视频大类',
  `video_type2`          STRING COMMENT '视频中类',
  `video_type3`          STRING COMMENT '视频小类',
  `video_url`            STRING COMMENT '视频url',
  `organ_name`           STRING COMMENT '视频所属机构名称',
  `video_sections_num`   BIGINT COMMENT '视频节数',
  `video_index_page`     BIGINT COMMENT '视频所在页数',
  `price`                BIGINT COMMENT '收费信息(单位：分)',
  `person_num`           BIGINT COMMENT '购买/报名人数',
  `recently_study_num`   BIGINT COMMENT '最近在学',
  `sign_up_num`          BIGINT COMMENT '累计报名',
  `video_praise_degree`  DOUBLE COMMENT '好评度',
  `etl_time`             STRING COMMENT '数据加工时间'
) 
PARTITIONED BY (`dt` STRING) 
STORED AS PARQUET