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