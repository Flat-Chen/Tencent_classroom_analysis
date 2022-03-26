SELECT SPLIT(video_url, '/')[4] AS video_id,
       video_title,
       video_type1,         
       video_type2,         
       video_type3,                 
       video_url,           
       organ_name, 
       CAST(CASE WHEN video_sections_num IS NOT NULL THEN video_sections_num                        ELSE 0 END AS BIGINT) AS video_sections_num,  
       video_index_page,                         
       CAST(CASE WHEN price              IS NOT NULL THEN price                                     ELSE 0 END AS BIGINT) AS price,  
       CAST(CASE WHEN person_num         IS NOT NULL THEN person_num                                ELSE 0 END AS BIGINT) AS person_num,  
       CAST(CASE WHEN recently_study_num IS NOT NULL THEN recently_study_num                        ELSE 0 END AS BIGINT) AS recently_study_num,  
       CAST(CASE WHEN sign_up_num        IS NOT NULL THEN REPLACE(recently_study_num, '万', '0000') ELSE 0 END AS BIGINT) AS sign_up_num,  
       CASE WHEN video_praise_degree >'0'       THEN CAST(REPLACE(video_praise_degree, '%', '') AS INT)/100 ELSE 0 END    AS video_praise_degree,            
       FROM_UNIXTIME(CAST(NOW() AS BIGINT), 'yyyy-MM-dd HH:mm:ss') AS elt_time,
       DATE_FORMAT(TO_TIMESTAMP(grab_time, 'yyyy-MM-dd HH:mm:ss'), 'yyyy-MM-dd') AS dt 
  FROM ( 
         SELECT video_title,         
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
        