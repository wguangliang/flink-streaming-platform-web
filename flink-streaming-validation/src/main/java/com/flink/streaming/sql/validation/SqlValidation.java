package com.flink.streaming.sql.validation;

import com.flink.streaming.common.constant.SystemConstant;
import com.flink.streaming.common.enums.SqlCommand;
import com.flink.streaming.common.model.SqlCommandCall;
import com.flink.streaming.common.sql.SqlFileParser;
import com.flink.streaming.sql.util.ValidationConstants;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.sql.parser.validate.FlinkSqlConformance;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.bridge.java.internal.StreamTableEnvironmentImpl;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.table.delegation.Parser;
import org.apache.flink.table.operations.ModifyOperation;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.planner.calcite.CalciteConfig;
import org.apache.flink.table.planner.delegation.FlinkSqlParserFactories;
import org.apache.flink.table.planner.parse.CalciteParser;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.table.planner.utils.TableConfigUtils;

/**
 * 校验sql是否合法
 */
@Slf4j
public class SqlValidation {

    //TODO 暂时没有找到好的解决方案

    /**
     * @author zhuhuipei
     * @date 2021/3/27
     * @time 10:10
     *
     */
    public static void preCheckSql(List<String> sql) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();

        TableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

        List<SqlCommandCall> sqlCommandCallList = SqlFileParser.fileToSql(sql);
        if (CollectionUtils.isEmpty(sqlCommandCallList)) {
            throw new RuntimeException("没解析出sql，请检查语句 如 缺少;号");
        }

        TableConfig config = tEnv.getConfig();
        String value = null;

        boolean isInsertSql = false;

        boolean isSelectSql = false;
        try {
            for (SqlCommandCall sqlCommandCall : sqlCommandCallList) {

                value = sqlCommandCall.operands[0];

                switch (sqlCommandCall.sqlCommand) {
                    //配置
                    case SET:
                        String key = sqlCommandCall.operands[0];
                        String val = sqlCommandCall.operands[1];
                        if (val.contains(SystemConstant.LINE_FEED)) {
                            throw new RuntimeException("set 语法值异常：" + val);
                        }
                        /**
                         * 设置方言
                         */
                        // 处理设置sql方言的逻辑，比如 set table.sql-dialect=hive;  https://blog.csdn.net/weixin_45417821/article/details/124676446
                        if (TableConfigOptions.TABLE_SQL_DIALECT.key().equalsIgnoreCase(key.trim())
                                && SqlDialect.HIVE.name().equalsIgnoreCase(val.trim())) {
                            // 如果有设置sql方言，且是hive。则
                            config.setSqlDialect(SqlDialect.HIVE);
                        } else {
                            config.setSqlDialect(SqlDialect.DEFAULT);
                        }

                        break;
                    case BEGIN_STATEMENT_SET:
                    case END:
                        break;
                    /**
                     * 其他，使用 CalciteParser 进行sql解析校验
                     */
                    default:
                        if (SqlCommand.INSERT_INTO.equals(sqlCommandCall.sqlCommand)
                                || SqlCommand.INSERT_OVERWRITE.equals(sqlCommandCall.sqlCommand)) {
                            isInsertSql = true;
                        }
                        if (SqlCommand.SELECT.equals(sqlCommandCall.sqlCommand)) {
                            isSelectSql = true;
                        }
                        CalciteParser parser = new CalciteParser(getSqlParserConfig(config));
                        parser.parse(sqlCommandCall.operands[0]); // sqlCommandCall.operands[0]就是单个完整sql
                        break;
                }
            }
        } catch (Exception e) {
            log.warn("语法异常：  sql={}  原因是: {}", value, e);
            throw new RuntimeException("语法异常   sql=" + value + "  原因:   " + e.getMessage());
        }
        if (!isInsertSql) {
            throw new RuntimeException(ValidationConstants.MESSAGE_010);
        }

        if (isSelectSql) {
            throw new RuntimeException(ValidationConstants.MESSAGE_011);
        }

    }

    /**
     * 从sql文件中读取的每行内容
     * @param sql
     */
    public static void preCheckSql2(List<String> sql) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode()
                .useBlinkPlanner() // blink
                .build();
        StreamTableEnvironmentImpl ste = (StreamTableEnvironmentImpl) StreamTableEnvironment.create(env, settings);
        List<SqlCommandCall> sqlCommandCallList = SqlFileParser.fileToSql(sql);
        if (CollectionUtils.isEmpty(sqlCommandCallList)) {
            throw new RuntimeException("没解析出sql，请检查语句 如 缺少;号");
        }

        TableConfig config = ste.getConfig();
        String value = null;
        boolean isInsertSql = false;
        boolean isSelectSql = false;
        try {
            for (SqlCommandCall sqlCommandCall : sqlCommandCallList) {
                value = sqlCommandCall.operands[0];
                switch (sqlCommandCall.sqlCommand) {
                    // 配置
                    case SET:
                        String key = sqlCommandCall.operands[0];
                        String val = sqlCommandCall.operands[1];
                        if (val.contains(SystemConstant.LINE_FEED)) {
                            throw new RuntimeException("set 语法值异常：" + val);
                        }
                        /**
                         * 设置方言
                         */
                        // 处理设置sql方言的逻辑，比如 set table.sql-dialect=hive;  https://blog.csdn.net/weixin_45417821/article/details/124676446
                        if (TableConfigOptions.TABLE_SQL_DIALECT.key().equalsIgnoreCase(key.trim())
                                && SqlDialect.HIVE.name().equalsIgnoreCase(val.trim())) {
                            // 如果有设置sql方言，且是hive。则
                            config.setSqlDialect(SqlDialect.HIVE);
                        } else {
                            config.setSqlDialect(SqlDialect.DEFAULT);
                        }
                        // 上下文
                        ste.sqlUpdate(key+"="+val);
                        break;
                    case BEGIN_STATEMENT_SET:
                    case END:
                        break;
                    /**
                     * 其他，使用 CalciteParser 进行sql解析校验
                     */
                    default:
                        if (SqlCommand.INSERT_INTO.equals(sqlCommandCall.sqlCommand)
                                || SqlCommand.INSERT_OVERWRITE.equals(sqlCommandCall.sqlCommand)) {
                            isInsertSql = true;
                            // 上下文
                            sqlTranslate(ste, sqlCommandCall.operands[0], sql);
                        }
                        if (SqlCommand.SELECT.equals(sqlCommandCall.sqlCommand)) {
                            isSelectSql = true;
                            // 上下文
                            ste.sqlUpdate(sqlCommandCall.operands[0]);
                        } else {
                            ste.sqlUpdate(sqlCommandCall.operands[0]);
                        }
                        CalciteParser parser = new CalciteParser(getSqlParserConfig(config));
                        parser.parse(sqlCommandCall.operands[0]); // sqlCommandCall.operands[0]就是单个完整sql
                        break;
                }
            }
        } catch (Exception e) {
            log.warn("语法异常：  sql={}  原因是: {}", value, e);
            throw new RuntimeException("语法异常   sql=" + value + "  原因:   " + e.getMessage());
        }
        if (!isInsertSql) {
            throw new RuntimeException(ValidationConstants.MESSAGE_010);
        }

        if (isSelectSql) {
            throw new RuntimeException(ValidationConstants.MESSAGE_011);
        }

    }

    private static SqlParser.Config getSqlParserConfig(TableConfig tableConfig) {
        return JavaScalaConversionUtil.toJava(getCalciteConfig(tableConfig).getSqlParserConfig()).orElseGet(
                () -> {
                    SqlConformance conformance = getSqlConformance(tableConfig.getSqlDialect());
                    return SqlParser
                            .config()
                            .withParserFactory(FlinkSqlParserFactories.create(conformance))
                            .withConformance(conformance)
                            .withLex(Lex.JAVA)
                            .withIdentifierMaxLength(256);
                }
        );
    }

    private static CalciteConfig getCalciteConfig(TableConfig tableConfig) {
        return TableConfigUtils.getCalciteConfig(tableConfig);
    }

    private static FlinkSqlConformance getSqlConformance(SqlDialect sqlDialect) {
        switch (sqlDialect) {
            case HIVE:
                return FlinkSqlConformance.HIVE;
            case DEFAULT:
                return FlinkSqlConformance.DEFAULT;
            default:
                throw new TableException("Unsupported SQL dialect: " + sqlDialect);
        }
    }

    /**
     * 字符串转sql
     */
    public static List<String> toSqlList(String sql) {
        if (StringUtils.isEmpty(sql)) {
            return Collections.emptyList();
        }
        return Arrays.asList(sql.split(SystemConstant.LINE_FEED));
    }

    /**
     * 获取环境的Planner并验证
     * @param ste
     * @param sql
     * @throws Exception
     */
    private static void sqlTranslate(StreamTableEnvironmentImpl ste, String sql, List<String> sqlList) throws Exception {
        /** 获取环境的Planner并验证 **/
        org.apache.flink.table.delegation.Planner planner = ste.getPlanner();
        Parser parser = ste.getPlanner().getParser();
        try {
            List<Operation> operation = parser.parse(sql);
            planner.translate(Collections.singletonList((ModifyOperation) operation.get(0)));
        } catch (Exception e) {
            // TODO: handle exception
            System.err.println(e.toString());
            String errInfo = "出现语法错误：" + e.toString().split(": ")[1].split("Was")[0];
            errInfo = e.toString().split(": ").length > 2 ? errInfo + " : " + e.toString().split(": ")[2] : errInfo;
            throw new Exception(errInfo);
        }
    }

}
