
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;


public class FlinkFunction {
    public static void main(String[] args) throws Exception {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        //基于nc -lk 端口 的形式
        String hostname = parameterTool.get("hostname");
        int port = parameterTool.getInt("port");
        //创建流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //默认并行度为计算机内核数，window为12核，yarn的nodemanager默认8核
//        env.setParallelism(6);
        /* checkpoint相关配置 */
        env.enableCheckpointing(6000);
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        /* checkpoint模式：精确一致性*/
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        /* checkpoint间隔: 6000毫秒 */
        checkpointConfig.setMinPauseBetweenCheckpoints(6000);
        /* checkpoint超时时间: 60秒 */
        checkpointConfig.setCheckpointTimeout(60000);
        /* 无论作业失败或取消，均保留 checkpoint */
//        checkpointConfig.enableExternalizedCheckpoints(
//                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
//        /* 设置状态后端,异步快照，checkpoint运行时state保存在taskmanager JVM内存中，checkpoint时持久化到文件系统中 */
////        env.setStateBackend(new HashMapStateBackend());  //checkpointing中,state保存在taskmanager的JVM内存中
////        checkpointConfig.setCheckpointStorage(new FileSystemCheckpointStorage("file:///checkpoint-dir"));//checkpoint时，将分布式的state保存在文件系统中
//        env.setStateBackend(new FsStateBackend("hdfs://HAnameservices/flink/checkpoints/rsk/test"));

        DataStreamSource<String> dataStreamSource = env.socketTextStream(hostname, port);
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = dataStreamSource.flatMap(new rflatmap())
                .keyBy(new rKeySelector())
                .sum(1);
//                .sum(1).setParallelism(2).slotSharingGroup("red");
        sum.print();
        env.execute("flink job on yarn 大道至简");

    }


    public static class rflatmap implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String s, Collector<Tuple2<String, Integer>> out) throws Exception {
            String[] s1 = s.split(" ");
            for (String str : s1) {
                out.collect(Tuple2.of(str, 1));
            }
        }
    }

    public static class rKeySelector implements KeySelector<Tuple2<String, Integer>, String> {
        @Override
        public String getKey(Tuple2<String, Integer> tuple) throws Exception {
            return tuple.f0;
        }
    }

}
