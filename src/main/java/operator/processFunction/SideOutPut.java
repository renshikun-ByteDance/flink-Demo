package operator.processFunction;

import operator.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;


/**
 * flink的分流操作：
 *
 * <p>
 * filter: DataStream → DataStream，需要对同一个数据流，遍历多次，浪费集群资源，影响效率
 * <p>
 * 3.Side Outputs: DataStream → DataStream"s"，旁路输出，官网推荐用法
 * <p>
 * split: 已经被弃用，官方推荐使用Side Outputs（旁路分流），所以目前flink 1.12.1只支撑filter和SideOutPut 旁路分流了
 * <p>
 * 注意：
 * ###分流操作和分区操作的区别
 *
 *
 *
 * <p>
 * connect: DataStream,DataStream → ConnectedStreams，连接两个数据流，两个数据流被 Connect 之后，只是被放在了一个同一个流中，内部依然保持各自的数据和形式不发生任何变化，两个流相互独立
 * 注意: connect只能合并两条流，两条流的数据类型可以不同，可以通过coMap将其转换为一致的即可
 * <p>
 * union: DataStream* → DataStream，对两个或者两个以上的 DataStream 进行 union 操作，产生一个包含所有 DataStream 元素的新 DataStream
 * 注意: union可以合并多条流，多条流的数据类型必须相同
 * <p>
 *
 *
 * <p>
 * https://nightlies.apache.org/flink/flink-docs-release-1.12/zh/dev/stream/side_output.html
 */
public class SideOutPut {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //source
        DataStreamSource<String> inputStream = env.readTextFile("D:\\IDEAproject\\flink-Demo\\src\\main\\resources\\sensor.txt");


        //转换算子map，将数据转换为SensorReading对象
        SingleOutputStreamOperator<SensorReading> dataStream = inputStream.map(value -> {
            String[] results = value.split(",");
            return new SensorReading(results[0], Long.parseLong(results[1]), Double.parseDouble(results[2]));
        });

        //1. 定义一个OutputTag，用于表示旁路输出流
        OutputTag<SensorReading> lowTemp = new OutputTag<SensorReading>("lowTemp") {
        };

        //2. 基于ProcessFunction自定义分流操作，注意：此处基于匿名内部类，且匿名内部类中无法重载构造函数，因此多用在无需给传递参数时
        SingleOutputStreamOperator<SensorReading> originalStreams = dataStream.process(new ProcessFunction<SensorReading, SensorReading>() {
            //process function 可以通过 Context 对象发射一个事件到一个或者多个 side outputs
            @Override
            public void processElement(SensorReading value, Context ctx, Collector<SensorReading> out) throws Exception {
                //判断温度，30度作为高温和低温流的分界点，高温流输出到主流，低温流输出到侧输出流
                if (value.getTemperature() > 30) {   //基于流中的每个元素进行分流操作
                    //输出到主流
                    out.collect(value);               //out分流后的主流（高温流）
                } else {
                    //输出到侧流
                    ctx.output(lowTemp, value);       //ctx分流后的侧输出流（低温流）
                }
            }
        });

//        dataStream.print("main-temp");   //"原始流"

        originalStreams.print("high-temp");     //原始流，分流操作后的主流（由于分流操作，剔除了部分元素）
        originalStreams.getSideOutput(lowTemp).print("low-temp");    //侧输出流，分流操作，从原始流中过滤出来的部分数据

        DataStream<Tuple2<String, Double>> warningStream = originalStreams.map(new MapFunction<SensorReading, Tuple2<String, Double>>() {
            @Override
            public Tuple2<String, Double> map(SensorReading value) throws Exception {
                return new Tuple2<>(value.getId(), value.getTemperature());
            }
        });

        //合流操作，两条流的数据类型不同
        ConnectedStreams<Tuple2<String, Double>, SensorReading> connectStream = warningStream.connect(originalStreams.getSideOutput(lowTemp));

        //针对合流进行处理（一国两制，最终可转为同一种数据类型，也可各自独有数据类型（Object））
        DataStream<Object> resultStream = connectStream.map(new CoMapFunction<Tuple2<String, Double>, SensorReading, Object>() {
            @Override
            public Object map1(Tuple2<String, Double> value) throws Exception {
                return new Tuple3<>(value.f0, value.f1, "high temp warning");
            }
            @Override
            public Object map2(SensorReading value) throws Exception {
                return new Tuple2<>(value.getId(), "normal");
            }
        });

        resultStream.print("合流");
        env.execute();
    }
}
