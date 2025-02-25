package com.flink.streaming.core.execute;

import com.flink.streaming.common.model.SqlCommandCall;
import com.flink.streaming.core.config.Configurations;
import com.flink.streaming.core.logs.LogPrint;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.TableEnvironment;

import java.util.List;

/**
 * @author zhuhuipei
 * @Description:
 * @date 2021/3/21
 * @time 17:29
 */
public class ExecuteSql {

    /**
     * 将不同的sql类型，装入sql操作集合statementSet，主要分
     * SET 配置类： org.apache.flink.configuration.Configuration 进行配置
     * INSERT 插入类： statementSet.addInsertSql插入
     * 显示类： console打印
     * 其他：例如CREATE类：  tEnv.executeSql 执行
     * @author zhuhuipei
     * @date 2021/3/21
     * @time 17:33
     */
    public static void exeSql(List<SqlCommandCall> sqlCommandCallList, TableEnvironment tEnv, StatementSet statementSet) {
        for (SqlCommandCall sqlCommandCall : sqlCommandCallList) {
            switch (sqlCommandCall.sqlCommand) {
                //配置
                case SET:
                    Configurations.setSingleConfiguration(tEnv, sqlCommandCall.operands[0],
                            sqlCommandCall.operands[1]);
                    break;
                //insert 语句
                case INSERT_INTO:
                case INSERT_OVERWRITE:
                    LogPrint.logPrint(sqlCommandCall);
                    statementSet.addInsertSql(sqlCommandCall.operands[0]);
                    break;
                //显示语句
                case SELECT:
                case SHOW_CATALOGS:
                case SHOW_DATABASES:
                case SHOW_MODULES:
                case SHOW_TABLES:
                    LogPrint.queryRestPrint(tEnv, sqlCommandCall);
                    break;
                // 兼容sql-client.sh的用法，只显示但不执行
                case BEGIN_STATEMENT_SET:
                case END:
                    LogPrint.logPrint(sqlCommandCall);
                    break;
                default:
                    LogPrint.logPrint(sqlCommandCall);
                    tEnv.executeSql(sqlCommandCall.operands[0]);
                    break;
            }
        }
    }
}
