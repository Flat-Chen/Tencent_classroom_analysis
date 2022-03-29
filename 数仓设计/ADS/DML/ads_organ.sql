SELECT organ_name,
       max_study_video,
       avg_page,
       add_person_num_10d,
       add_person_num_10d_max_video
  FROM txkt.dws_organ
 WHERE dt='{dt}'