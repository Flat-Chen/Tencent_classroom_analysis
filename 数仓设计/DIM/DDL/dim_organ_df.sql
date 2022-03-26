CREATE TABLE IF NOT EXISTS txkt.dim_organ_df
 (
  `organ_name`           STRING COMMENT '机构名称',
  `organ_praise_degree`  STRING COMMENT '机构好评度',
  `organ_video_num`      STRING COMMENT '机构课程数',
  `organ_all_person_num` STRING COMMENT '机构总学习人次',
  `etl_time`             STRING COMMENT '数据加工时间'      
) 
PARTITIONED BY (`dt` STRING) 
STORED AS PARQUET