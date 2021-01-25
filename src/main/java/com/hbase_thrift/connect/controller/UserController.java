package com.hbase_thrift.connect.controller;

import com.hbase_thrift.connect.service.UserService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author huanghuajie
 * @version 1.0
 * @date 2021/1/25 15:07
 */

@RestController
@RequestMapping("/user")
public class UserController {

    @Autowired
    private UserService userService;

    @GetMapping("/userinfo")
    public Object getUser() {
        return userService.getByRowkey("krb_hbase_test01", "001");
    }
}
