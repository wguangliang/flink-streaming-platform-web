package com.flink.streaming.common.model;


import com.flink.streaming.common.enums.SqlCommand;
import lombok.Data;

/**
 * @author zhuhuipei
 * @Description:
 * @date 2020-06-23
 * @time 02:56
 */
@Data
public class SqlCommandCall {
    /**
     * sql类型的枚举。根据不同的enum，有不同的识别匹配逻辑
      */
    public SqlCommand sqlCommand;
    /**
     * 如果是SET类型 operands[0]是key，operands[1]是value
     * 如果是其他类型 operands[0]是单个完整sql。
     */
    public String[] operands;

    public SqlCommandCall(SqlCommand sqlCommand, String[] operands) {
        this.sqlCommand = sqlCommand;
        this.operands = operands;
    }

    public SqlCommandCall(String[] operands) {
        this.operands = operands;
    }
}
