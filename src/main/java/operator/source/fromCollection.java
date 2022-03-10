package operator.source;

import operator.beans.student;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;
import java.util.List;

public class fromCollection {

    public static void main(String[] args) throws Exception {

        //创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);//并行度为1，数据按顺序输出

        //自定义source，fromCollection从集合中获取
        DataStreamSource<student> studentDataStreamSource = env.fromCollection(Arrays.asList(
                new student("1","任世坤", 28, "13526408999", "北京市区丰台区"),
                new student("1","张三", 26, "13526608889", "青海海西藏族自治区"),
                new student("1","王五", 29, "11111111111", "河南省郑州市"),
                new student("1","老刘", 26, "11111111111", "河南省郑州市")
        ));
        //自定义source，fromElements从要素中获取
        //注意上下的区别
        DataStreamSource<student> ElementSource = env.fromElements(
                new student("1","王老五", 28, "13526408999", "北京市区丰台区"),
                new student("1","王老六", 26, "13526608889", "青海海西藏族自治区"),
                new student("1","王老七", 29, "11111111111", "河南省郑州市"),
                new student("1","王老八", 26, "11111111111", "河南省郑州市")
        );

        //        DataStreamSource<List<student>> ElementSource = env.fromElements(Arrays.asList(
//                new student("王老五", 28, "13526408999", "北京市区丰台区"),
//                new student("王老六", 26, "13526608889", "青海海西藏族自治区"),
//                new student("王老七", 29, "11111111111", "河南省郑州市"),
//                new student("王老八", 26, "11111111111", "河南省郑州市")
//        ));

        //sink
        studentDataStreamSource.print("student");
        ElementSource.print("Elements");

        //运行作业
        env.execute("fromCollection demo");

    }


}
