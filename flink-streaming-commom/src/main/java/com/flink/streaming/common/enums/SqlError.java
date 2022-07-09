package com.flink.streaming.common.enums;

import com.flink.streaming.common.constant.SystemConstant;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author: wangguangliang
 * @date: Create in 2022/7/1 2:34 下午
 * @description
 */
@Getter
public enum SqlError {

    OBJECT_NOT_FOUND (
            // 入参0：sql
            (input) -> {
                String regex = "(.*?). From line (.*?), column .*? to line (.*?), column .*? : Object '(.*?)' not found";
                Pattern pattern = Pattern.compile(regex, SystemConstant.DEFAULT_PATTERN_FLAGS);
                String sql = input[0];
                Matcher matcher = pattern.matcher(sql);
                StringBuffer sqlBuffer = new StringBuffer();
                if (matcher.find()) {
                    for (int i =0; i<=matcher.groupCount(); i++) {
                        System.out.println("i = "+ i +", " +matcher.group(i));
                    }
                    String errorMessage = matcher.group(1); // SQL validation failed
                    String lineNum = matcher.group(2);      // 行数
                    String notFoundVal = matcher.group(4);  // source_table2
                    sqlBuffer.append(sql.substring(matcher.start(0), matcher.start(2)));
                    sqlBuffer.append("tihuan");
                    sqlBuffer.append(sql.substring(matcher.end(2), matcher.start(3)));
                    sqlBuffer.append("tihuan");
                    sqlBuffer.append(sql.substring(matcher.end(3), matcher.end()));
                    System.out.println(sqlBuffer);
                }
                return Optional.of(input);
            }
    );


    public final Function<String[], Optional<String[]>> operandConverter;
    //                sql
    SqlError(Function<String[], Optional<String[]>> operandConverter) {
        this.operandConverter = operandConverter;
    }

    public static void main(String[] args) {
        String[] input = new String[]{"SQL validation failed. From line 1, column 46 to line 1, column 58 : Object 'source_table2' not found"};
        Optional<String[]> apply = OBJECT_NOT_FOUND.operandConverter.apply(input);
        System.out.println(StringUtils.join(apply.get(), ","));
    }

    /**
     * 按group位置进行字符串替换
     */
    public String groupReplace(Matcher matcher, String str, List<Integer> indexes, List<String> replaceValues) {
        assert indexes.size() == replaceValues.size();
        StringBuffer stringBuffer = new StringBuffer();
        if (matcher.find()) {
            int idx = indexes.get(0);
            stringBuffer.append(str.substring(matcher.start(0), matcher.start(idx)))
                    .append(replaceValues.get(0));
            for (int i = 1; i < indexes.size(); i++) {
                idx = indexes.get(i);
                stringBuffer.append(str.substring(matcher.end(indexes.get(i-1)), matcher.start(idx)))
                        .append(replaceValues.get(i));
            }
            stringBuffer.append(str.substring(matcher.end(idx), matcher.end()));
        }
        return stringBuffer.toString();
    }
}
