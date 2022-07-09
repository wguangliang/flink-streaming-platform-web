package com.flink.streaming.core;


import com.flink.streaming.common.constant.SystemConstant;
import com.flink.streaming.common.enums.JobTypeEnum;
import com.flink.streaming.common.model.SqlCommandCall;
import com.flink.streaming.common.sql.SqlFileParser;
import com.flink.streaming.core.checkpoint.CheckPointParams;
import com.flink.streaming.core.checkpoint.FsCheckPoint;
import com.flink.streaming.core.execute.ExecuteSql;
import com.flink.streaming.core.model.JobRunParam;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.calcite.shaded.com.google.common.base.Preconditions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

/**
 * @author zhuhuipei
 * @Description:
 * @date 2020-06-23
 * @time 00:33
 * flink core 提交程序入口
 * com.flink.streaming.core.JobApplication 这个就是真正运行的JOB,在IDEA 里面也是可以执行的，配置一下参数就可以了，相信大家可以举一反三的
 *    我debug时用的参数参考： -sql /Users/gump/study/source/github/flink-streaming-platform-web/sql/job_sql_1.sql  -type 0
 *    这里有个细节：core pom中的Flink 依赖包是 provide 的，本地要debug job时可以 注释这个刷新Maven
 *
 * 我们在任务中配置的SQL，会生成在项目的 /sql 目录下 ，也就是上面命令的 -sql 后的路径 -type 是告诉任务是流任务还是 批任务
 */
public class JobApplication {

    private static final Logger log = LoggerFactory.getLogger(JobApplication.class);

    public static void main(String[] args) {

        try {
            Arrays.stream(args).forEach(arg -> log.info("{}", arg));
            /**
             * 程序运行的参数： sql文件的目录，
             *               任务类型：SQL_STREAMING(0), JAR(1), SQL_BATCH(2);
             *               checkpoint:CheckPointParam
             */
            JobRunParam jobRunParam = buildParam(args);
            // 从sql文件目录中读取sql语句
            List<String> sql = Files.readAllLines(Paths.get(jobRunParam.getSqlPath()));
            // 读取每一行的语句，一个分号结尾的作为一条完整的sql，进行parse()解析定位类型。每个完整的sql放到List<SqlCommandCall>
            List<SqlCommandCall> sqlCommandCallList = SqlFileParser.fileToSql(sql);

            EnvironmentSettings settings = null;

            TableEnvironment tEnv = null;
            /**
             * 根据不同的环境配置，进行初始化TableEnvironment
             * 批
             * 流
             * jar 的呢？
             */
            if (jobRunParam.getJobTypeEnum() != null && JobTypeEnum.SQL_BATCH.equals(jobRunParam.getJobTypeEnum())) {
                log.info("[SQL_BATCH]本次任务是批任务");
                //批处理
                settings = EnvironmentSettings.newInstance()
                        .useBlinkPlanner()
                        .inBatchMode()
                        .build();
                tEnv = TableEnvironment.create(settings);
            } else {
                log.info("[SQL_STREAMING]本次任务是流任务");
                //默认是流 流处理 目的是兼容之前版本
                StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

                settings = EnvironmentSettings.newInstance()
                        .useBlinkPlanner()
                        .inStreamingMode()
                        .build();
                tEnv = StreamTableEnvironment.create(env, settings);
                //设置checkPoint
                FsCheckPoint.setCheckpoint(env, jobRunParam.getCheckPointParam());

            }
            /**
             * 创建sql执行
             */
            // 创建sql操作集合
            StatementSet statementSet = tEnv.createStatementSet();
            // 配置sql操作集合
            ExecuteSql.exeSql(sqlCommandCallList, tEnv, statementSet);
            // 执行sql操作集合
            TableResult tableResult = statementSet.execute();
            /**
             判断执行的效果
             */
            if (tableResult == null || tableResult.getJobClient().get() == null
                    || tableResult.getJobClient().get().getJobID() == null) {
                throw new RuntimeException("任务运行失败 没有获取到JobID");
            }
            JobID jobID = tableResult.getJobClient().get().getJobID();

            System.out.println(SystemConstant.QUERY_JOBID_KEY_WORD + jobID);

            log.info(SystemConstant.QUERY_JOBID_KEY_WORD + "{}", jobID);

        } catch (Exception e) {
            System.err.println("任务执行失败:" + e.getMessage());
            log.error("任务执行失败：", e);
        }


    }

    /**
     * 传参主要有2个参数
     * -sql    sql文件的本地目录
     * -type   0:SQL_STREAMING, 1:JAR, 2:SQL_BATCH
     * @param args
     * @return
     * @throws Exception
     */
    private static JobRunParam buildParam(String[] args) throws Exception {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String sqlPath = parameterTool.get("sql");
        Preconditions.checkNotNull(sqlPath, "-sql参数 不能为空");
        JobRunParam jobRunParam = new JobRunParam();
        jobRunParam.setSqlPath(sqlPath);
        jobRunParam.setCheckPointParam(CheckPointParams.buildCheckPointParam(parameterTool));
        String type = parameterTool.get("type");
        if (StringUtils.isNotEmpty(type)) {
            jobRunParam.setJobTypeEnum(JobTypeEnum.getJobTypeEnum(Integer.valueOf(type)));
        }
        return jobRunParam;
    }

}
