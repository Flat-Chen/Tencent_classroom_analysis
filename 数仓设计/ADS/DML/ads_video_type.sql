SELECT video_type,
       max_study_video,
       add_study_num_10d,
       add_person_num_10d,
       add_person_num_10d_max_video
  FROM txkt.dws_video_type
 WHERE dt='{dt}'