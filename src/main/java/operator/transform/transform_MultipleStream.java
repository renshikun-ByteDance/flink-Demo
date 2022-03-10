package operator.transform;

import operator.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class transform_MultipleStream {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //source
        DataStreamSource<String> inputStream = env.readTextFile("D:\\IDEAproject\\flink-Demo\\src\\main\\resources\\sensor.txt");

        //转换算子-map
        SingleOutputStreamOperator<SensorReading> mapStream = inputStream.map(value -> {
            String[] results = value.split(",");
            return new SensorReading(results[0], Long.parseLong(results[1]), Double.parseDouble(results[2]));
        });

        //1. 分流，基于温度分流，条件为30度
//        mapStream






    }


}
