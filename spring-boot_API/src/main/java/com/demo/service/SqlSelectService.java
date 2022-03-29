package com.demo.service;

import com.zaxxer.hikari.HikariDataSource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class SqlSelectService {

    public List<Map<String,Object>> queryList(String sql){

        HikariDataSource dataSource = new HikariDataSource();
        dataSource.setDriverClassName("com.mysql.cj.jdbc.Driver");
        dataSource.setJdbcUrl("jdbc:mysql://node01:3306/txkt?useUnicode=true&characterEncoding=utf8&zeroDateTimeBehavior=convertToNull&tinyInt1isBit=false");
        dataSource.setUsername("root");
        dataSource.setPassword("@WSX3edc");
        dataSource.setMaximumPoolSize(20);
        dataSource.setIdleTimeout(60000);
        JdbcTemplate jdbcTemplate = new JdbcTemplate();
        jdbcTemplate.setDataSource(dataSource);
        jdbcTemplate.setFetchSize(10000);
        List<Map<String, Object>> queryList = jdbcTemplate.queryForList(sql);
        dataSource.close();
        return queryList;
    }

    public Map<String,Object> organ(){
        Map<String,Object> resultMap = new HashMap<>();
        List<Map<String, Object>>  max_study_video = queryList("select t.organ_name as name,t.max_study_video as value from (\n" +
                "SELECT organ_name, max_study_video, @rank := @rank + 1 as pm\n" +
                "FROM ads_organ A,\n" +
                "(SELECT @rank := 0) B\n" +
                "ORDER BY max_study_video DESC\n" +
                ") t where t.pm<=10;");

        resultMap.put("max_study_video",max_study_video);

        List<Map<String, Object>>  avg_page = queryList("select t.organ_name as name,t.avg_page as value from (\n" +
                "SELECT organ_name, avg_page, @rank := @rank + 1 as pm\n" +
                "FROM ads_organ A,\n" +
                "(SELECT @rank := 0) B\n" +
                "ORDER BY avg_page ASC\n" +
                ") t where t.pm<=10;");

        resultMap.put("avg_page",avg_page);

        List<Map<String, Object>>  add_person_num_10d = queryList("select t.organ_name as name,t.add_person_num_10d as value from (\n" +
                "SELECT organ_name, add_person_num_10d, @rank := @rank + 1 as pm\n" +
                "FROM ads_organ A,\n" +
                "(SELECT @rank := 0) B\n" +
                "ORDER BY add_person_num_10d DESC\n" +
                ") t where t.pm<=10;");
        resultMap.put("add_person_num_10d",add_person_num_10d);

        List<Map<String, Object>>  add_person_num_10d_max_video = queryList("select t.organ_name as name,t.add_person_num_10d_max_video as value from (\n" +
                "SELECT organ_name, add_person_num_10d_max_video, @rank := @rank + 1 as pm\n" +
                "FROM ads_organ A,\n" +
                "(SELECT @rank := 0) B\n" +
                "ORDER BY add_person_num_10d_max_video DESC\n" +
                ") t where t.pm<=10;");
        resultMap.put("add_person_num_10d_max_video",add_person_num_10d_max_video);
        return resultMap;
    }

    public Map<String,Object> video(){
        Map<String,Object> resultMap = new HashMap<>();
        List<Map<String, Object>>  add_study_num_10d = queryList("select t.video_title as name,t.add_study_num_10d as value from (\n" +
                "SELECT video_title, add_study_num_10d, @rank := @rank + 1 as pm\n" +
                "FROM ads_video A,\n" +
                "(SELECT @rank := 0) B\n" +
                "ORDER BY add_study_num_10d DESC\n" +
                ") t where t.pm<=10;");

        resultMap.put("add_study_num_10d",add_study_num_10d);

        List<Map<String, Object>>  add_person_num_10d = queryList("select t.video_title as name,t.add_person_num_10d as value from (\n" +
                "SELECT video_title, add_person_num_10d, @rank := @rank + 1 as pm\n" +
                "FROM ads_video A,\n" +
                "(SELECT @rank := 0) B\n" +
                "ORDER BY add_person_num_10d DESC\n" +
                ") t where t.pm<=10;");
        resultMap.put("add_person_num_10d",add_person_num_10d);
        return resultMap;
    }
    public Map<String,Object> video_type(){
        Map<String,Object> resultMap = new HashMap<>();
        List<Map<String, Object>>  max_study_video = queryList("select t.video_type as name,t.max_study_video as value from (\n" +
                "SELECT video_type, max_study_video, @rank := @rank + 1 as pm\n" +
                "FROM ads_video_type A,\n" +
                "(SELECT @rank := 0) B\n" +
                "ORDER BY max_study_video DESC\n" +
                ") t where t.pm<=10;");

        resultMap.put("max_study_video",max_study_video);

        List<Map<String, Object>>  add_study_num_10d = queryList("select t.video_type as name,t.add_study_num_10d as value from (\n" +
                "SELECT video_type, add_study_num_10d, @rank := @rank + 1 as pm\n" +
                "FROM ads_video_type A,\n" +
                "(SELECT @rank := 0) B\n" +
                "ORDER BY add_study_num_10d DESC\n" +
                ") t where t.pm<=10;");

        resultMap.put("add_study_num_10d",add_study_num_10d);

        List<Map<String, Object>>  add_person_num_10d = queryList("select t.video_type as name,t.add_person_num_10d as value from (\n" +
                "SELECT video_type, add_person_num_10d, @rank := @rank + 1 as pm\n" +
                "FROM ads_video_type A,\n" +
                "(SELECT @rank := 0) B\n" +
                "ORDER BY add_person_num_10d DESC\n" +
                ") t where t.pm<=10;");
        resultMap.put("add_person_num_10d",add_person_num_10d);

        List<Map<String, Object>>  add_person_num_10d_max_video = queryList("select t.video_type as name,t.add_person_num_10d_max_video as value from (\n" +
                "SELECT video_type, add_person_num_10d_max_video, @rank := @rank + 1 as pm\n" +
                "FROM ads_video_type A,\n" +
                "(SELECT @rank := 0) B\n" +
                "ORDER BY add_person_num_10d_max_video DESC\n" +
                ") t where t.pm<=10;");
        resultMap.put("add_person_num_10d_max_video",add_person_num_10d_max_video);
        return resultMap;
    }
}
