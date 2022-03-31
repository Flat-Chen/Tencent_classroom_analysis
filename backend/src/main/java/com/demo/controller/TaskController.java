package com.demo.controller;

import com.demo.service.SqlSelectService;
import io.swagger.annotations.Api;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Sort;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/rank")
public class TaskController {

    @Autowired
    private SqlSelectService sqlSelectService;



    @GetMapping("/organ")
    public Map<String,Object> organ()
    {
        return sqlSelectService.organ();
    }

    @GetMapping("/video")
    public Map<String,Object> video()
    {
        return sqlSelectService.video();
    }

    @GetMapping("/video_type")
    public Map<String,Object> video_type()
    {
        return sqlSelectService.video_type();
    }
}
