package com.hbase_thrift.connect.hbase_config;

import com.github.CCweixiao.AdminCallback;
import com.github.CCweixiao.MutatorCallback;
import com.github.CCweixiao.TableCallback;
import com.github.CCweixiao.exception.HBaseOperationsException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;

/**
 * @author huanghuajie
 * @version 1.0
 * @date 2021/1/25 13:59
 */
public interface HBaseKerberosOperations {
    /**
     * 获取HBase的连接对象
     *
     * @return 获取HBase连接
     */
    Connection getConnection();

    /**
     * 处理管理员类型的操作
     *
     * @param action 管理员类型的操作
     * @param <T>    泛型类型
     * @return 结果
     */
    default <T> T execute(AdminCallback<T> action) {
        Admin admin = null;
        try {
            admin = this.getConnection().getAdmin();
            return action.doInAdmin(admin);
        } catch (Throwable throwable) {
            throw new HBaseOperationsException(throwable);
        } finally {
            if (null != admin) {
                try {
                    admin.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * 处理数据读写类型的操作
     *
     * @param tableName 表名
     * @param action    表级别的读写操作回调
     * @param <T>       泛型类型
     * @return 结果
     */
    default <T> T execute(String tableName, TableCallback<T> action) {
        Table table = null;
        try {
            table = this.getConnection().getTable(TableName.valueOf(tableName));
            return action.doInTable(table);
        } catch (Throwable throwable) {
            throw new HBaseOperationsException(throwable);
        } finally {
            if (null != table) {
                try {
                    table.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * 处理数据批量读写类型的操作
     *
     * @param tableName 表名
     * @param action    批量读写类型操作的回调
     */
    default void execute(String tableName, MutatorCallback action) {
        BufferedMutator mutator = null;
        try {
            BufferedMutatorParams mutatorParams = new BufferedMutatorParams(TableName.valueOf(tableName));
            mutator = this.getConnection().getBufferedMutator(mutatorParams.writeBufferSize(4 * 1024 * 1024));
            action.doInMutator(mutator);
        } catch (Throwable throwable) {
            throw new HBaseOperationsException(throwable);
        } finally {
            if (null != mutator) {
                try {
                    mutator.flush();
                    mutator.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
