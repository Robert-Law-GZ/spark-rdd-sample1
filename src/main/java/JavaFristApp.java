import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.storage.StorageLevel;

import java.util.Arrays;

import java.util.List;
public class JavaFristApp {

    public static void main(String[] args) {
        String logFile="/usr/local/Cellar/spark/README.md";

        SparkConf conf=new SparkConf();
        conf.setAppName("Java Frist App");
        conf.setMaster("local[4]");
        JavaSparkContext sc=new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile(logFile);

        JavaRDD<String> sparks = lines.filter(line -> line.contains("Spark"));

        List<String> sparksItems = sparks.collect();
        for (String item:sparksItems){
            System.out.println(item);
        }

        System.out.println("\n count:"+sparks.count());


        JavaRDD<String[]> wordsRDD= lines.map(item -> item.split(" "));
        List<String[]> words = wordsRDD.collect();

        for (int i=0;i<words.size();i++){
            List<String> array = Arrays.asList(words.get(i));
            for (String a:array) {
                System.out.println("单词:" + a);
            }
        }

        JavaRDD<Integer> linesLengths = lines.map(s -> s.length());


        Integer ll = linesLengths.reduce(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer+integer2;
            }
        });

        System.out.println("count:"+ll);

        JavaRDD<Integer> list = linesLengths.persist(StorageLevel.MEMORY_ONLY());

        System.out.println(list.collect());
    }

}
