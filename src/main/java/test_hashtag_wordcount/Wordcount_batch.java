package test_hashtag_wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.util.Collector;

import java.util.List;

//batch processing test
public class Wordcount_batch {
    public static void main(String[] args) throws Exception {
        //创建运行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        String inputpath = "D:\\hashtag-Flink\\src\\main\\resources\\test.txt";
        DataSet<String> inputsource = env.readTextFile(inputpath);
        //处理数据，使数据输出位一个二元组(word,1)进行统计
        DataSet<Tuple2<String,Integer>> resultSet = inputsource.flatMap(new MyFlatMapper())
                   .groupBy(0)//按照第一个位置的word分组
                   .sum(1);//把第二个位置的数字求和

        resultSet.print();


    }
    //自定义类，实现flatmapfunction接口，注意接口中的泛型
    public static class MyFlatMapper implements FlatMapFunction<String, Tuple2<String,Integer>>, ListCheckpointed<Integer> {

        public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
            String[] words = s.split(" ");
            //遍历所有word，包装成二元组进行输出
            for(String str:words){
                collector.collect(new Tuple2<String, Integer>(str,1));
            }

        }

        @Override
        public List<Integer> snapshotState(long l, long l1) throws Exception {
            return null;
        }

        @Override
        public void restoreState(List<Integer> list) throws Exception {

        }
    }


}
