package com.hbase_thrift.connect.service;

import com.github.CCweixiao.HBaseAdminTemplate;
import com.github.CCweixiao.HBaseTemplate;
import com.hbase_thrift.connect.hbase_config.HBaseKerberosTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Map;

/**
 * @author huanghuajie
 * @version 1.0
 * @date 2021/1/25 10:06
 */

@Service
public class UserService {
    @Autowired
    private HBaseTemplate hBaseTemplate;
    @Autowired
    private HBaseAdminTemplate hBaseAdminTemplate;
    @Autowired
    private HBaseKerberosTemplate hBaseKerberosTemplate;

//    public Map<String, Map<String, String>> getByRowKey(String rowkey) {
//        return (Map<String, Map<String, String>>) hBaseTemplate.getByRowKey(rowkey, Map.class);
//    }
//
    public Object getByRowkey(String tableName, String rowkey) {
        return hBaseKerberosTemplate.getByRowKey(tableName, rowkey);
    }
}
