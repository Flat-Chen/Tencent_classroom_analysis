from pyspark.sql import SparkSession
import time


today = time.strftime("%Y-%m-%d", time.localtime()) 

spark = SparkSession.builder.enableHiveSupport().getOrCreate()

# ODS 
tb_df = spark.read \
    .format("jdbc") \
    .option("driver", "com.mysql.jdbc.Driver") \
    .option("url", "jdbc:mysql://node01:3306/txkt") \
    .option("user", "root") \
    .option("password", "@WSX3edc") \
    .option("spark.sql.parquet.int96AsTimestamp",False) \
    .option("dbtable", 'tencent_study_video_spider_' + time.strftime("%Y_%m_%d", time.localtime())) \
    .load()
tb_df.createOrReplaceTempView("mysql_tencent_study_video_spider")

spark.sql(
    'INSERT OVERWRITE TABLE txkt.ods_tencent_study_video PARTITION (dt)' + 
    '''
         SELECT video_type1,         
                video_type2,         
                video_type3,         
                video_title,         
                video_url,           
                video_sections_num,  
                video_index_page,    
                organ_name,          
                price,               
                person_num,          
                recently_study_num,  
                sign_up_num,         
                video_praise_degree, 
                organ_praise_degree, 
                organ_video_num,     
                organ_all_person_num,
                grab_time,           
                FROM_UNIXTIME(CAST(NOW() AS BIGINT), 'yyyy-MM-dd HH:mm:ss') AS elt_time,
                DATE_FORMAT(TO_TIMESTAMP(grab_time, 'yyyy-MM-dd HH:mm:ss'), 'yyyy-MM-dd') AS dt
           FROM mysql_tencent_study_video_spider
    '''
)


#DIM
spark.sql(
    'INSERT OVERWRITE TABLE txkt.dim_video_df PARTITION (dt)' + 
    '''
        SELECT SPLIT(video_url, '/')[4] AS video_id,
               video_title, 
               video_type1,        
               video_type2,         
               video_type3,                 
               video_url,           
               organ_name, 
               CAST(CASE WHEN video_sections_num IS NOT NULL THEN video_sections_num ELSE 0 END AS BIGINT) AS video_sections_num,  
               video_index_page,  
               CAST(CASE WHEN price              IS NOT NULL THEN price              ELSE 0 END AS BIGINT) AS price,  
               CAST(CASE WHEN person_num         IS NOT NULL THEN person_num         ELSE 0 END AS BIGINT) AS person_num,  
               CAST(CASE WHEN recently_study_num IS NOT NULL THEN recently_study_num ELSE 0 END AS BIGINT) AS recently_study_num,  
               CAST(CASE WHEN sign_up_num IS NOT NULL THEN REPLACE(recently_study_num, '万', '0000') ELSE 0 END AS BIGINT) AS sign_up_num,  
               CASE WHEN video_praise_degree > '0' THEN CAST(REPLACE(video_praise_degree, '%', '') AS INT)/100 ELSE 0 END  AS video_praise_degree,            
               FROM_UNIXTIME(CAST(NOW() AS BIGINT), 'yyyy-MM-dd HH:mm:ss') AS elt_time,
               DATE_FORMAT(TO_TIMESTAMP(grab_time, 'yyyy-MM-dd HH:mm:ss'), 'yyyy-MM-dd') AS dt 
          FROM ( 
                 SELECT video_title,  
                        video_type1,       
                        video_type2,         
                        video_type3,                 
                        video_url,           
                        organ_name, 
                        video_sections_num,  
                        video_index_page,            
                        price,               
                        person_num,          
                        recently_study_num,  
                        sign_up_num,         
                        video_praise_degree,
                        grab_time,
                        ROW_NUMBER() OVER(PARTITION BY video_url ORDER BY grab_time DESC) AS rk
                   FROM txkt.ods_tencent_study_video
                  WHERE dt='{dt}' 
               ) AS a
         WHERE rk=1
    '''.format(dt = today)
) 

spark.sql(
    'INSERT OVERWRITE TABLE txkt.dim_organ_df PARTITION (dt)' + 
    '''
        SELECT organ_name,         
               organ_praise_degree,         
               organ_video_num,                 
               organ_all_person_num,          
               FROM_UNIXTIME(CAST(NOW() AS BIGINT), 'yyyy-MM-dd HH:mm:ss') AS elt_time,
               DATE_FORMAT(TO_TIMESTAMP(grab_time, 'yyyy-MM-dd HH:mm:ss'), 'yyyy-MM-dd') AS dt 
          FROM ( 
                 SELECT organ_name,         
                        organ_praise_degree,         
                        organ_video_num,                 
                        organ_all_person_num,           
                        grab_time,
                        ROW_NUMBER() OVER(PARTITION BY organ_name ORDER BY grab_time DESC) AS rk
                   FROM txkt.ods_tencent_study_video
                  WHERE dt='{dt}' 
               ) AS a
         WHERE rk=1
    '''.format(dt = today)
)

# DWS
spark.sql(
    '''
       INSERT OVERWRITE TABLE txkt.dws_video PARTITION (dt = "{dt}")
            SELECT today.video_id,
                   today.video_title,
                   (NVL(today.recently_study_num, 0) -  NVL(d1.recently_study_num, 0))          AS add_study_num_1d,
                   (NVL(today.recently_study_num, 0) -  NVL(d3.recently_study_num, 0))          AS add_study_num_3d,
                   (NVL(today.recently_study_num, 0) - NVL(d10.recently_study_num, 0))          AS add_study_num_10d,
                   (NVL(today.person_num, 0) -  NVL(d1.person_num, 0))                          AS add_person_num_1d,
                   (NVL(today.person_num, 0) -  NVL(d3.person_num, 0))                          AS add_person_num_3d,
                   (NVL(today.person_num, 0) - NVL(d10.person_num, 0))                          AS add_person_num_10d,
                   today.person_num                                    * today.price            AS profit_num_td,
                   (NVL(today.person_num, 0) -  NVL(d1.person_num, 0)) * today.price            AS profit_num_1d,
                   (NVL(today.person_num, 0) -  NVL(d3.person_num, 0)) * today.price            AS profit_num_3d,
                   (NVL(today.person_num, 0) - NVL(d10.person_num, 0)) * today.price            AS profit_num_10d,
                   FROM_UNIXTIME(CAST(NOW() AS BIGINT), 'yyyy-MM-dd HH:mm:ss')                  AS elt_time
              FROM (
                     SELECT video_id,
                            video_title,
                            recently_study_num, 
                            person_num,
                            price
                       FROM txkt.dim_video_df
                      WHERE dt = '{dt}'
                   ) AS today
         LEFT JOIN (
                     SELECT video_id,
                            video_title,
                            recently_study_num,
                            person_num,
                            price
                       FROM txkt.dim_video_df
                      WHERE dt = DATE_SUB('{dt}',1)
                   ) AS d1
                ON today.video_id = d1.video_id
         LEFT JOIN (
                     SELECT video_id,
                            video_title,
                            recently_study_num,
                            person_num,
                            price
                       FROM txkt.dim_video_df
                      WHERE dt = DATE_SUB('{dt}', 3)
                   ) AS d3
                ON today.video_id = d3.video_id
         LEFT JOIN (
                     SELECT video_id,
                            video_title,
                            recently_study_num,
                            person_num,
                            price
                       FROM txkt.dim_video_df
                      WHERE dt = DATE_SUB('{dt}', 10)
                   ) AS d10
                ON today.video_id = d10.video_id
    '''.format(dt = today)
)

spark.sql(
    '''
      INSERT OVERWRITE TABLE txkt.dws_organ PARTITION (dt = '{dt}')
      SELECT dim_organ.organ_name,
             charge_video_num,
             free_video_num,
             expensive_video_price,
             cheap_video_price,
             organ_all_person_num,
             organ_all_charge_person_num,
             max_study_video,
             avg_page,
             add_study_num_1d,
             add_study_num_3d,
             add_study_num_10d,
             add_study_num_1d_max_video,
             add_study_num_3d_max_video,
             add_study_num_10d_max_video,
             add_person_num_1d,
             add_person_num_3d,
             add_person_num_10d,
             add_person_num_1d_max_video,
             add_person_num_3d_max_video,
             add_person_num_10d_max_video,
             profit_num_td,
             profit_num_1d,
             profit_num_3d,
             profit_num_10d,
             FROM_UNIXTIME(CAST(NOW() AS BIGINT), 'yyyy-MM-dd HH:mm:ss')  AS elt_time
        FROM (
               SELECT organ_name,
                      CAST(organ_all_person_num AS BIGINT) AS organ_all_person_num
                 FROM txkt.dim_organ_df
                WHERE dt = '{dt}'
             ) AS dim_organ
   LEFT JOIN ( 
               SELECT organ_name,
                      SUM(CASE WHEN price > 0 THEN 1 ELSE 0 END)                               AS charge_video_num,
                      SUM(CASE WHEN price = 0 THEN 1 ELSE 0 END)                               AS free_video_num,
                      MAX(price)                                                               AS expensive_video_price,
                      MAX(CASE WHEN price > 0 THEN price ELSE NULL END)                        AS cheap_video_price,
                      SUM(CASE WHEN price > 0 THEN today.person_num ELSE 0 END)                AS organ_all_charge_person_num,
                      MAX(CASE WHEN study_video_rank = 1 THEN today.video_id ELSE NULL END)          AS max_study_video,
                      AVG(video_index_page)                                                    AS avg_page,
                      SUM((NVL(today.recently_study_num, 0) -  NVL(d1.recently_study_num, 0))) AS add_study_num_1d,
                      SUM((NVL(today.recently_study_num, 0) -  NVL(d3.recently_study_num, 0))) AS add_study_num_3d,
                      SUM((NVL(today.recently_study_num, 0) - NVL(d10.recently_study_num, 0))) AS add_study_num_10d,
                      SUM((NVL(today.person_num, 0) -  NVL(d1.person_num, 0)))                 AS add_person_num_1d,
                      SUM((NVL(today.person_num, 0) -  NVL(d3.person_num, 0)))                 AS add_person_num_3d,
                      SUM((NVL(today.person_num, 0) - NVL(d10.person_num, 0)))                 AS add_person_num_10d,
                      SUM(today.person_num                                    * price)         AS profit_num_td,
                      SUM((NVL(today.person_num, 0) -  NVL(d1.person_num, 0)) * price)         AS profit_num_1d,
                      SUM((NVL(today.person_num, 0) -  NVL(d3.person_num, 0)) * price)         AS profit_num_3d,
                      SUM((NVL(today.person_num, 0) - NVL(d10.person_num, 0)) * price)         AS profit_num_10d
                 FROM (
                         SELECT video_id,
                                recently_study_num, 
                                person_num,
                                price,
                                organ_name,
                                video_index_page,
                                ROW_NUMBER() OVER(PARTITION BY organ_name ORDER BY person_num DESC) AS study_video_rank
                           FROM txkt.dim_video_df
                          WHERE dt = '{dt}'
                      ) AS today
            LEFT JOIN (
                        SELECT video_id,
                               recently_study_num, 
                               person_num
                          FROM txkt.dim_video_df
                         WHERE dt = DATE_SUB('{dt}',1)
                      ) AS d1
                   ON today.video_id = d1.video_id
            LEFT JOIN (
                        SELECT video_id,
                               recently_study_num, 
                               person_num
                          FROM txkt.dim_video_df
                         WHERE dt = DATE_SUB('{dt}', 3)
                      ) AS d3
                   ON today.video_id = d3.video_id
            LEFT JOIN (
                        SELECT video_id,
                               recently_study_num, 
                               person_num
                          FROM txkt.dim_video_df
                         WHERE dt = DATE_SUB('{dt}', 10)
                      ) AS d10
                   ON today.video_id = d10.video_id
             GROUP BY organ_name
             ) AS organ_info
          ON dim_organ.organ_name = organ_info.organ_name
   LEFT JOIN (
               SELECT organ_name,
                      MAX(CASE WHEN add_study_num_1d_rk  = 1 THEN dws_video.video_id ELSE NULL END ) AS add_study_num_1d_max_video,
                      MAX(CASE WHEN add_study_num_3d_rk  = 1 THEN dws_video.video_id ELSE NULL END ) AS add_study_num_3d_max_video,
                      MAX(CASE WHEN add_study_num_10d_rk = 1 THEN dws_video.video_id ELSE NULL END ) AS add_study_num_10d_max_video,
                      MAX(CASE WHEN add_person_num_1d_rk = 1 THEN dws_video.video_id ELSE NULL END ) AS add_person_num_1d_max_video,
                      MAX(CASE WHEN add_person_num_3d_rk = 1 THEN dws_video.video_id ELSE NULL END ) AS add_person_num_3d_max_video,
                      MAX(CASE WHEN add_person_num_10_rk = 1 THEN dws_video.video_id ELSE NULL END ) AS add_person_num_10d_max_video
                 FROM (
                        SELECT video_id,
                               ROW_NUMBER() OVER(PARTITION BY video_id ORDER BY add_study_num_1d   DESC )AS add_study_num_1d_rk,
                               ROW_NUMBER() OVER(PARTITION BY video_id ORDER BY add_study_num_3d   DESC )AS add_study_num_3d_rk,
                               ROW_NUMBER() OVER(PARTITION BY video_id ORDER BY add_study_num_10d  DESC )AS add_study_num_10d_rk,
                               ROW_NUMBER() OVER(PARTITION BY video_id ORDER BY add_person_num_1d  DESC )AS add_person_num_1d_rk,
                               ROW_NUMBER() OVER(PARTITION BY video_id ORDER BY add_person_num_3d  DESC )AS add_person_num_3d_rk,
                               ROW_NUMBER() OVER(PARTITION BY video_id ORDER BY add_person_num_10d DESC )AS add_person_num_10_rk
                          FROM txkt.dws_video 
                         WHERE dt = '{dt}'
                      ) AS dws_video
           INNER JOIN (
                        SELECT video_id,
                               organ_name
                          FROM txkt.dim_video_df
                         WHERE dt = '{dt}'
                      ) AS dim_video
                   ON dws_video.video_id = dim_video.video_id
             GROUP BY organ_name
             ) AS video_info 
          ON dim_organ.organ_name = video_info.organ_name
    '''.format(dt = today)
)

spark.sql(
    '''
        INSERT OVERWRITE TABLE txkt.dws_video_type PARTITION (dt = '{dt}')
          SELECT video_type_info.video_type1,
           charge_video_num,
           free_video_num,
           expensive_video_price,
           cheap_video_price,
           all_person_num,
           all_charge_person_num,
           max_study_video,
           add_study_num_1d,
           add_study_num_3d,
           add_study_num_10d,
           add_study_num_1d_max_video,
           add_study_num_3d_max_video,
           add_study_num_10d_max_video,
           add_person_num_1d,
           add_person_num_3d,
           add_person_num_10d,
           add_person_num_1d_max_video,
           add_person_num_3d_max_video,
           add_person_num_10d_max_video,
           profit_num_td,
           profit_num_1d,
           profit_num_3d,
           profit_num_10d,
           FROM_UNIXTIME(CAST(NOW() AS BIGINT), 'yyyy-MM-dd HH:mm:ss')  AS elt_time
      FROM ( 
             SELECT video_type1,
                    SUM(CASE WHEN price > 0 THEN 1 ELSE 0 END)                               AS charge_video_num,
                    SUM(CASE WHEN price = 0 THEN 1 ELSE 0 END)                               AS free_video_num,
                    MAX(price)                                                               AS expensive_video_price,
                    MAX(CASE WHEN price > 0 THEN price ELSE NULL END)                        AS cheap_video_price,
                    SUM(today.person_num)                                                    AS all_person_num,
                    SUM(CASE WHEN price > 0 THEN today.person_num ELSE 0 END)                AS all_charge_person_num,
                    MAX(CASE WHEN study_video_rank = 1 THEN today.video_id ELSE NULL END)    AS max_study_video,
                    SUM((NVL(today.recently_study_num, 0) -  NVL(d1.recently_study_num, 0))) AS add_study_num_1d,
                    SUM((NVL(today.recently_study_num, 0) -  NVL(d3.recently_study_num, 0))) AS add_study_num_3d,
                    SUM((NVL(today.recently_study_num, 0) - NVL(d10.recently_study_num, 0))) AS add_study_num_10d,
                    SUM((NVL(today.person_num, 0) -  NVL(d1.person_num, 0)))                 AS add_person_num_1d,
                    SUM((NVL(today.person_num, 0) -  NVL(d3.person_num, 0)))                 AS add_person_num_3d,
                    SUM((NVL(today.person_num, 0) - NVL(d10.person_num, 0)))                 AS add_person_num_10d,
                    SUM(today.person_num                                    * price)         AS profit_num_td,
                    SUM((NVL(today.person_num, 0) -  NVL(d1.person_num, 0)) * price)         AS profit_num_1d,
                    SUM((NVL(today.person_num, 0) -  NVL(d3.person_num, 0)) * price)         AS profit_num_3d,
                    SUM((NVL(today.person_num, 0) - NVL(d10.person_num, 0)) * price)         AS profit_num_10d
               FROM (
                       SELECT video_id,
                              recently_study_num, 
                              person_num,
                              price,
                              video_type1,
                              video_index_page,
                              ROW_NUMBER() OVER(PARTITION BY video_type1 ORDER BY person_num DESC) AS study_video_rank
                         FROM txkt.dim_video_df
                        WHERE dt = '{dt}'
                    ) AS today
          LEFT JOIN (
                      SELECT video_id,
                             recently_study_num, 
                             person_num
                        FROM txkt.dim_video_df
                       WHERE dt = DATE_SUB('{dt}',1)
                    ) AS d1
                 ON today.video_id = d1.video_id
          LEFT JOIN (
                      SELECT video_id,
                             recently_study_num, 
                             person_num
                        FROM txkt.dim_video_df
                       WHERE dt = DATE_SUB('{dt}', 3)
                    ) AS d3
                 ON today.video_id = d3.video_id
          LEFT JOIN (
                      SELECT video_id,
                             recently_study_num, 
                             person_num
                        FROM txkt.dim_video_df
                       WHERE dt = DATE_SUB('{dt}', 10)
                    ) AS d10
                 ON today.video_id = d10.video_id
           GROUP BY video_type1
           ) AS video_type_info
 LEFT JOIN (
             SELECT video_type1,
                    MAX(CASE WHEN add_study_num_1d_rk  = 1 THEN dws_video.video_id ELSE NULL END ) AS add_study_num_1d_max_video,
                    MAX(CASE WHEN add_study_num_3d_rk  = 1 THEN dws_video.video_id ELSE NULL END ) AS add_study_num_3d_max_video,
                    MAX(CASE WHEN add_study_num_10d_rk = 1 THEN dws_video.video_id ELSE NULL END ) AS add_study_num_10d_max_video,
                    MAX(CASE WHEN add_person_num_1d_rk = 1 THEN dws_video.video_id ELSE NULL END ) AS add_person_num_1d_max_video,
                    MAX(CASE WHEN add_person_num_3d_rk = 1 THEN dws_video.video_id ELSE NULL END ) AS add_person_num_3d_max_video,
                    MAX(CASE WHEN add_person_num_10_rk = 1 THEN dws_video.video_id ELSE NULL END ) AS add_person_num_10d_max_video
               FROM (
                      SELECT video_id,
                             ROW_NUMBER() OVER(PARTITION BY video_id ORDER BY add_study_num_1d   DESC )AS add_study_num_1d_rk,
                             ROW_NUMBER() OVER(PARTITION BY video_id ORDER BY add_study_num_3d   DESC )AS add_study_num_3d_rk,
                             ROW_NUMBER() OVER(PARTITION BY video_id ORDER BY add_study_num_10d  DESC )AS add_study_num_10d_rk,
                             ROW_NUMBER() OVER(PARTITION BY video_id ORDER BY add_person_num_1d  DESC )AS add_person_num_1d_rk,
                             ROW_NUMBER() OVER(PARTITION BY video_id ORDER BY add_person_num_3d  DESC )AS add_person_num_3d_rk,
                             ROW_NUMBER() OVER(PARTITION BY video_id ORDER BY add_person_num_10d DESC )AS add_person_num_10_rk
                        FROM txkt.dws_video 
                       WHERE dt = '{dt}'
                    ) AS dws_video
         INNER JOIN (
                      SELECT video_id,
                             video_type1
                        FROM txkt.dim_video_df
                       WHERE dt = '{dt}'
                    ) AS dim_video
                 ON dws_video.video_id = dim_video.video_id
           GROUP BY video_type1
           ) AS video_info 
        ON video_type_info.video_type1 = video_info.video_type1
    '''.format(dt = today)
)


# ADS

spark.sql(
    '''
       INSERT OVERWRITE TABLE txkt.ads_video
       SELECT video_id,
              video_title,
              add_study_num_10d,
              add_person_num_10d
         FROM txkt.dws_video
        WHERE dt='{dt}'
    '''.format(dt = today)
)

spark.sql(
    '''
        INSERT OVERWRITE TABLE txkt.ads_organ
        SELECT organ_name,
               max_study_video,
               avg_page,
               add_person_num_10d,
               add_person_num_10d_max_video
          FROM txkt.dws_organ
         WHERE dt='{dt}'
    '''.format(dt = today)
)

spark.sql(
    '''
        INSERT OVERWRITE TABLE txkt.ads_video_type
        SELECT video_type,
               max_study_video,
               add_study_num_10d,
               add_person_num_10d,
               add_person_num_10d_max_video
          FROM txkt.dws_video_type
         WHERE dt='{dt}'
    '''.format(dt = today)
)


df_video = spark.sql(
    '''
        SELECT *
          FROM txkt.ads_video
    '''
)


df_organ = spark.sql(
    '''
        SELECT *
          FROM txkt.ads_organ
    '''
)

df_video_type = spark.sql(
    '''
        SELECT *
          FROM txkt.ads_video_type
    '''
)


df_video.write.mode("overwrite") \
  .format("jdbc") \
  .option("url", "jdbc:mysql://node01:3306/txkt") \
  .option("dbtable", "ads_video") \
  .option("user", "root") \
  .option("password", "@WSX3edc") \
  .save()

df_organ.write.mode("overwrite") \
  .format("jdbc") \
  .option("url", "jdbc:mysql://node01:3306/txkt") \
  .option("dbtable", "ads_organ") \
  .option("user", "root") \
  .option("password", "@WSX3edc") \
  .save()

df_video_type.write.mode("overwrite") \
  .format("jdbc") \
  .option("url", "jdbc:mysql://node01:3306/txkt") \
  .option("dbtable", "ads_video_type") \
  .option("user", "root") \
  .option("password", "@WSX3edc") \
  .save()

spark.stop()
