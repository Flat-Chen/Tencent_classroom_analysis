   SELECT dim_organ.organ_name,
          charge_video_num,
          free_video_num,
          expensive_video_price,
          cheap_video_price,
          organ_all_person_num,
          organ_all_charge_person_num,
          max_stduy_video,
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
                   MAX(CASE WHEN stduy_video_rank = 1 THEN today.video_id ELSE NULL END)          AS max_stduy_video,
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
                             ROW_NUMBER() OVER(PARTITION BY organ_name ORDER BY person_num DESC) AS stduy_video_rank
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
