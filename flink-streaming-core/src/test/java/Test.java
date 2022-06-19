import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

import com.flink.streaming.common.model.SqlCommandCall;
import com.flink.streaming.common.sql.SqlFileParser;
import com.flink.streaming.core.logs.LogPrint;

/**
 * @author zhuhuipei
 * @Description:
 * @date 2020-07-20
 * @time 21:36
 */
public class Test {

    public static void main(String[] args) throws IOException {
        System.out.println(File.separator);   // 文件系统的路径分隔符。 On UNIX systems the value of this field is <code>'/'</code>; on Microsoft Windows systems it is <code>'\\'</code>.

        ClassLoader threadClassLoader = Thread.currentThread().getContextClassLoader();
        System.out.println(threadClassLoader);
        //
        threadClassLoader.getResourceAsStream("");  // bin是本项目的classpath，所有的java源文件编译后的.class文件复制到这个目录中

//        List<String> sql = Files.readAllLines(Paths.get("D:\\ideaprojects\\flink-test\\src\\main\\resources\\online.sql"));
        List<String> sql = Files.readAllLines(Paths.get("flink-streaming-core/src/test/java/online.sql"));

        List<SqlCommandCall> sqlCommandCallList = SqlFileParser.fileToSql(sql);
        for (SqlCommandCall sqlCommandCall : sqlCommandCallList) {
            LogPrint.logPrint(sqlCommandCall);
        }
    }

    @org.junit.Test
    public void sqlFileParser() throws IOException {
        // 目录定位test
//        File file = new File("src/test/java/abc.txt");
//        File file2 = new File("abc.txt");
//        if (!file2.exists()) {
//            System.out.println("create new file");
//            file2.createNewFile();
//        }





        List<String> sql = Files.readAllLines(Paths.get("src/test/java/online.sql"));

        List<SqlCommandCall> sqlCommandCallList = SqlFileParser.fileToSql(sql);
        for (SqlCommandCall sqlCommandCall : sqlCommandCallList) {
            LogPrint.logPrint(sqlCommandCall);
        }
    }
}
