package operator.transform;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * 主要介绍了基础的算子，包括：
 * <p>
 * map: DataStream → DataStream，对流中的每个元素进行"转换"（如大小写、转换为pojo类等），流中元素数量不变
 * <p>
 * flatMap: DataStream → DataStream，对流中的每个元素"拆分"，输入一个元素，产生0个、1个或者多个输出，流中元素数据增加
 * <p>
 * filter: DataStream → DataStream，对流中的元素进行"过滤"，对流中元素进行过滤，流中元素减少
 *
 *
 * Tuple类型是flink独有的数据类型，不是java的数据类型
 *
 *
 *
 *
 */
public class transform_Base {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //source
        DataStreamSource<String> inputStream = env.fromCollection(Arrays.asList(
                "renshikun,28,青海海北",
                "wulili,28,海拉尔",
                "rendongxun,3,青海海西",
                "rendongliang,0,新疆乌鲁木齐"
        ));

        //1.1 基础转换算子: map
        SingleOutputStreamOperator<String> mapStream = inputStream.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                return value.replace(",", "|");
            }
        });

        //1.2 基础平坦转换算子: flatMap
        SingleOutputStreamOperator<String> flatMapStream = inputStream.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] results = value.split(",");
                for (String re : results)
                    out.collect(re);
            }
        });

        //1.3 基础过滤算子: filter，基于匿名内部类来实现（不能传参），如果需要传参数，可单独定义一个类，在new对象时通过传参和构造函数的方式来传递参数
        SingleOutputStreamOperator<String> filterStream = inputStream.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String value) throws Exception {
                return value.contains("青海");
            }
        });
        //1.3 基础过滤算子: filter，基于lambda表达式来实现
        SingleOutputStreamOperator<String> flink = inputStream.filter(value -> value.contains("flink"));

        //sink
        filterStream.print();
        mapStream.print();
        flatMapStream.print();

        env.execute("transform");

    }
}
