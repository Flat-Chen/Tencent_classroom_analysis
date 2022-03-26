   SELECT video_type_info.video_type1,
          charge_video_num,
          free_video_num,
          expensive_video_price,
          cheap_video_price,
          all_person_num,
          all_charge_person_num,
          max_stduy_video,
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
                   MAX(CASE WHEN stduy_video_rank = 1 THEN today.video_id ELSE NULL END)    AS max_stduy_video,
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
                             ROW_NUMBER() OVER(PARTITION BY video_type1 ORDER BY person_num DESC) AS stduy_video_rank
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
