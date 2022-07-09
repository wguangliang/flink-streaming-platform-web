package com.flink.streaming.common.sql;

import com.flink.streaming.common.constant.SystemConstant;
import com.flink.streaming.common.enums.SqlCommand;
import com.flink.streaming.common.model.SqlCommandCall;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.regex.Matcher;

/**
 * @author zhuhuipei
 * @Description:
 * @date 2020-06-23
 *
 * @time 02:22
 * 将每行的语句转为每个完整的sql语句，并定义为 SqlCommand enum
 */
public class SqlFileParser {

    /**
     * 读取每一行的语句，一个分号结尾的作为一条完整的sql，进行parse()解析定位类型
     * 每一条完整的sql存到 List<SqlCommandCall>中
     * @param lineList
     * @return
     */
    public static List<SqlCommandCall> fileToSql(List<String> lineList) {

        if (CollectionUtils.isEmpty(lineList)) {
            throw new RuntimeException("lineList is null");
        }

        List<SqlCommandCall> sqlCommandCallList = new ArrayList<>();

        StringBuilder stmt = new StringBuilder();

        for (String lines : lineList) { // 遍历每一行，直到遇到分号，则进行sql解析一次 parse方法
            // 原代码如果两条完整的sql写到一行了，就会只定义一条sql。兼容如果一行数据中有多条完整的sql
            String[] lineSplitBySemicolon = lines.split(";");
            for (String line : lineSplitBySemicolon) {


                //开头是 -- 的表示注释
                if (line.trim().isEmpty() || line.startsWith(SystemConstant.COMMENT_SYMBOL) ||
                        trimStart(line).startsWith(SystemConstant.COMMENT_SYMBOL)) {
                    continue;
                }
                // 强行末位增加分号，但是存在如果末位没有分号的异常sql也会补一个分号
                line =  line + SystemConstant.SEMICOLON;

                stmt.append(SystemConstant.LINE_FEED).append(line);  // 换行
                if (line.trim().endsWith(SystemConstant.SEMICOLON)) { // trim后如果末位是分号;
                    Optional<SqlCommandCall> optionalCall = parse(stmt.toString());
                    if (optionalCall.isPresent()) {
                        sqlCommandCallList.add(optionalCall.get());
                    } else {
                        throw new RuntimeException("不支持该语法使用" + stmt.toString() + "'");
                    }
                    stmt.setLength(0); // 下一条sql开始前，清空原有的sql buffer。
                } // 如果末位没有分号，则不做处理
            }

        }

        return sqlCommandCallList;

    }

    private static Optional<SqlCommandCall> parse(String stmt) {
        stmt = stmt.trim();
        if (stmt.endsWith(SystemConstant.SEMICOLON)) {
            // 如果末位是分号，则去掉
            stmt = stmt.substring(0, stmt.length() - 1).trim();
        }
        for (SqlCommand cmd : SqlCommand.values()) { // 遍历sql类型enum
            final Matcher matcher = cmd.getPattern().matcher(stmt);
            if (matcher.matches()) {
                final String[] groups = new String[matcher.groupCount()];
                for (int i = 0; i < groups.length; i++) {
                    groups[i] = matcher.group(i + 1); // 提前1位，意思是第0位不要了，即明细不需要
                }
                System.out.println("group0:" + matcher.group(0));
                System.out.println("group1:" + matcher.group(1));
                return cmd.getOperandConverter().apply(groups)
                        .map((operands) -> new SqlCommandCall(cmd, operands));
            }
        }
        return Optional.empty();
    }

    /**
     * 只做trim开头的部分
     * @param str
     * @return
     */
    private static String trimStart(String str) {
        if (StringUtils.isEmpty(str)) {
            return str;
        }
        final char[] value = str.toCharArray();

        int start = 0, last = 0 + str.length() - 1;
        int end = last;
        while ((start <= end) && (value[start] <= ' ')) {
            start++;
        }
        if (start == 0 && end == last) {
            return str;
        }
        if (start >= end) {
            return "";
        }
        return str.substring(start, end);
    }
}
