package operator.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class fromFile {

    public static void main(String[] args) throws Exception {
        //创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //source
        DataStreamSource<String> dataStream = env.readTextFile("C:\\Users\\lenovo\\Desktop\\student.txt");
        //sink
        dataStream.print();
        //开始执行环境
        env.execute("dataStream from file");
    }
}
