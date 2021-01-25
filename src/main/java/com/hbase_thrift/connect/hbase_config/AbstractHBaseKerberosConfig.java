package com.hbase_thrift.connect.hbase_config;

import com.github.CCweixiao.HBaseOperations;
import com.github.CCweixiao.exception.HBaseOperationsException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;

import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

/**
 * @author huanghuajie
 * @version 1.0
 * @date 2021/1/25 13:58
 */
public class AbstractHBaseKerberosConfig implements HBaseOperations {
    private static final Logger LOGGER = LoggerFactory.getLogger(HBaseOperations.class);
    private Configuration configuration;

    private volatile Connection connection;

    @Value("${spring.data.hbase.kerberos.kerb5}")
    private String krb5Conf;

    @Value("${spring.data.hbase.kerberos.principal}")
    private String principal;

    @Value("${spring.data.hbase.kerberos.keytab}")
    private String keyTab;

    public AbstractHBaseKerberosConfig() {
        this.configuration = getConfiguration();
    }

    public AbstractHBaseKerberosConfig(Configuration configuration) {
        if (configuration == null) {
            throw new HBaseOperationsException("a valid configuration is provided.");
        }
        this.configuration = configuration;
    }

    public AbstractHBaseKerberosConfig(String zkHost, String zkPort) {
        Configuration configuration = getConfiguration(zkHost, zkPort);
        if (configuration == null) {
            throw new HBaseOperationsException("a valid configuration is provided.");
        }
        this.configuration = configuration;
    }

    public AbstractHBaseKerberosConfig(Properties properties) {
        Configuration configuration = getConfiguration(properties);
        if (configuration == null) {
            throw new HBaseOperationsException("a valid configuration is provided.");
        }
        this.configuration = configuration;
    }


    @Override
    public Connection getConnection() {
        if (null == this.connection) {
            synchronized (this) {
                if (null == this.connection) {
                    try {
                        System.setProperty("java.security.krb5.conf", krb5Conf);
                        UserGroupInformation.setConfiguration(configuration);
                        UserGroupInformation.loginUserFromKeytab(principal, keyTab);
                        this.connection = ConnectionFactory.createConnection(configuration, Executors.newFixedThreadPool(80));
                        LOGGER.info("the connection pool of HBase is created successfully.>>>>>>>>>>>>>>>>>>");
                    } catch (IOException e) {
                        LOGGER.error("the connection pool of HBase is created failed.>>>>>>>>>>>>>>>>>");
                        LOGGER.error(e.getMessage());
                    }
                }
            }
        }
        return this.connection;
    }

    public void setConnection(Connection connection) {
        this.connection = connection;
    }

    public Configuration getConfiguration() {
        this.configuration = HBaseConfiguration.create();
        configuration.addResource("core-site.xml");
        configuration.addResource("hbase-site.xml");
        configuration.addResource("hdfs-site.xml");
        configuration.set("hadoop.security.authentication", "kerberos");
        return configuration;
    }

    public void setConfiguration(Configuration configuration) {
        this.configuration = configuration;
    }

    public Configuration getConfiguration(String zkHost, String zkPort) {
        this.configuration = HBaseConfiguration.create();
        configuration.set(HConstants.ZOOKEEPER_QUORUM, zkHost);
        configuration.set(HConstants.ZOOKEEPER_CLIENT_PORT, zkPort);
        return configuration;
    }

    public Configuration getConfiguration(Properties properties) {
        this.configuration = HBaseConfiguration.create();
        final List<String> keys = properties.keySet().stream().map(Object::toString).collect(Collectors.toList());
        keys.forEach(key -> configuration.set(key, properties.getProperty(key)));
        return configuration;
    }
}
