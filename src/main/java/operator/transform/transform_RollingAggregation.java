package operator.transform;

import operator.beans.SensorReading;
import operator.beans.student;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


/**
 * 滚动聚合算子仅能针对单个字段进行处理？
 * <p>
 * 主要介绍了"滚动聚合算子"，针对 KeyedStream 的每一个支流做聚合，包括：
 * <p>
 * KeyBy: DataStream → KeyedStream，将一个流拆分成不相交的分区，每个分区包含具有相同 key 的元素，在内部以 hash 的形式实现的，相同Key的元素仅会在某个分区中，但某个分区中会有多个Key的元素
 * <p>
 * sum: KeyedStream → DataStream，类同min/max，在流中"更新"的最新元素中指定字段的值，其余字段值不变，同min和max
 * <p>
 * min: KeyedStream → DataStream，在流中"更新"的最新元素中的字段，即将最新元素中的相关字段"更新"为其最小值（单个元素不完整，拼接元素构成）
 * minBy: KeyedStream → DataStream，在流中"过滤"元素，返回该字段中具有最小值的元素（元素完整）
 * <p>
 * max: KeyedStream → DataStream
 * maxBy: KeyedStream → DataStream
 * <p>
 * Reduce: KeyedStream → DataStream，一个分组数据流的聚合操作，合并当前的元素和上次聚合的结果，产生一个新的值，返回的流中包含每一次聚合的结果，而不是只返回最后一次聚合的最终结果
 * <p>
 */
public class transform_RollingAggregation {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //如果并行度不给1，则采用CPU的核心数作为默认的并行度
        env.setParallelism(1);  //Reduce算子话，要考虑算子并行度的问题，并行度不是1的话，会有数据乱序的问题

        //source
        DataStreamSource<String> inputStream = env.readTextFile("D:\\IDEAproject\\flink-Demo\\src\\main\\resources\\sensor.txt");

        //基础转换算子： map
        SingleOutputStreamOperator<SensorReading> dataStream = inputStream.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String value) throws Exception {
                String[] results = value.split(",");
                return new SensorReading(results[0], Long.parseLong(results[1]), Double.parseDouble(results[2]));
            }
        });

        //数据流分区算子：KeyBy
        KeyedStream<SensorReading, String> keyByStream = dataStream.keyBy(SensorReading::getId);//基于Id对数据流进行分区

        //1.1滚动聚合算子：sum
        SingleOutputStreamOperator<SensorReading> sumStream = keyByStream.sum("temperature");
        //1.2滚动聚合算子：max和maxBy
        SingleOutputStreamOperator<SensorReading> maxStream = keyByStream.max("temperature");
        SingleOutputStreamOperator<SensorReading> maxByStream = keyByStream.maxBy("temperature");//maxBy(SensorReading::getTemperature);//为什么不行

        //1.3滚动聚合算子：min和minBy
        SingleOutputStreamOperator<SensorReading> minStream = keyByStream.min("temperature");
        SingleOutputStreamOperator<SensorReading> minByStream = keyByStream.minBy("temperature");

        //1.4滚动聚合算子：reduce
        SingleOutputStreamOperator<SensorReading> reduceStream = keyByStream.reduce(new ReduceFunction<SensorReading>() {
            @Override
            public SensorReading reduce(SensorReading value1, SensorReading value2) throws Exception {
                //value1:上一版数据，value2:新数据
                return new SensorReading(value1.getId(), value2.getTimestamp(), Math.max(value1.getTemperature(), value2.getTemperature()));
            }
        });

//        sumStream.print("sumStream");
//        maxStream.print("maxStream");
//        maxByStream.print("maxByStream");
//        minStream.print("minStream");
//        minByStream.print("minByStream");
        reduceStream.print("reduceStream");

        env.execute("滚动聚合算子");

    }


}


//        keyBy基于索引方式只能用于Tuple元组
//        KeyedStream<student, Tuple> keyByStream = dataStream.keyBy(0);
//        KeyedStream<student, Tuple> keyByStream = dataStream.keyBy("adress");
//        SingleOutputStreamOperator<student> dataStream = inputStream.map(line -> {
//            String[] results = line.split(",");
//            return new student(results[0], results[1], Integer.parseInt(results[2]), results[3], results[4]);
//        });