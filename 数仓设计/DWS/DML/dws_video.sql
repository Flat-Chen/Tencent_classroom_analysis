   SELECT today.video_id,
          today.video_title,

          (NVL(today.recently_study_num, 0) -  NVL(d1.recently_study_num, 0))          AS add_study_num_1d,
          (NVL(today.recently_study_num, 0) -  NVL(d3.recently_study_num, 0))          AS add_study_num_3d,
          (NVL(today.recently_study_num, 0) - NVL(d10.recently_study_num, 0))          AS add_study_num_10d,

          (NVL(today.person_num, 0) -  NVL(d1.person_num, 0))                          AS add_person_num_1d,
          (NVL(today.person_num, 0) -  NVL(d3.person_num, 0))                          AS add_person_num_3d,
          (NVL(today.person_num, 0) - NVL(d10.person_num, 0))                          AS add_person_num_10d,

          today.person_num                                    * price            AS profit_num_td,
          (NVL(today.person_num, 0) -  NVL(d1.person_num, 0)) * price            AS profit_num_1d,
          (NVL(today.person_num, 0) -  NVL(d3.person_num, 0)) * price            AS profit_num_3d,
          (NVL(today.person_num, 0) - NVL(d10.person_num, 0)) * price            AS profit_num_10d,

          FROM_UNIXTIME(CAST(NOW() AS BIGINT), 'yyyy-MM-dd HH:mm:ss')  AS elt_time
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