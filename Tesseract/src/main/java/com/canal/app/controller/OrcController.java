package com.canal.app.controller;

import org.springframework.core.io.FileSystemResource;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.*;
import com.canal.app.service.OCRServiceImpl;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.multipart.MultipartFile;

import javax.annotation.Resource;

/**
 * @program: FlinkSql
 * @description:
 * @author: yang
 * @create: 2021-04-13 10:43
 */
@RestController
@RequestMapping("/orc")
public class OrcController {

    @Resource
    private OCRServiceImpl  service;

    @Resource
    RestTemplate restTemplate;

    @PostMapping("/pic")
    public String getPic(@RequestParam(value = "file") MultipartFile file) {
       return service.getCharacterFromPic(file);
    }


    @GetMapping("/test")
    public String test() {
        return "test";
    }



    @PostMapping("/pic1")
    public String getPic1(@RequestParam(value = "file") MultipartFile file) {
        final String filePath = "F:";
        final String fileName = "testFile.txt";
        final String url = "http://182.254.232.175:8080";

        RestTemplate restTemplate = new RestTemplate();

        //设置请求头
        HttpHeaders headers = new HttpHeaders();
        MediaType type = MediaType.parseMediaType("multipart/form-data");
        headers.setContentType(type);

        //设置请求体，注意是LinkedMultiValueMap
        FileSystemResource fileSystemResource = new FileSystemResource(filePath+"/"+fileName);
        MultiValueMap<String, Object> form = new LinkedMultiValueMap<>();
        form.add("file", fileSystemResource);
        form.add("filename",fileName);

        //用HttpEntity封装整个请求报文
        HttpEntity<MultiValueMap<String, Object>> files = new HttpEntity<>(form, headers);

        String s = restTemplate.postForObject(url, files, String.class);
        System.out.println(s);
        return null;
    }



}
