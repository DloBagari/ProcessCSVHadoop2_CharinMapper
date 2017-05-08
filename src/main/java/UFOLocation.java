import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.ChainMapper;
import org.apache.hadoop.mapred.lib.LongSumReducer;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class UFOLocation {
    public static class MapClass extends MapReduceBase implements Mapper<LongWritable, Text, Text, LongWritable> {
        private Pattern pattern = Pattern.compile("[a-zA-Z]{2}[^a-zA-Z]*$");
        private LongWritable one = new LongWritable(1);
        public void map(LongWritable key, Text value, OutputCollector<Text,
                LongWritable> outputCollector, Reporter reporter) throws IOException {
            String[] columns = value.toString().split(",");
            Matcher matcher = pattern.matcher(columns[2].trim());
            if (matcher.find()) {
                int startIndex = matcher.start();
                outputCollector.collect(new Text(columns[2].substring(startIndex, startIndex + 2)), one);
            }
        }
    }

    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
        JobConf jobConf = new JobConf(conf, UFOLocation.class);
        jobConf.setJobName("UFO states");
        jobConf.setOutputKeyClass(Text.class);
        jobConf.setOutputValueClass(LongWritable.class);

        JobConf jobConf1 = new JobConf(false);
        ChainMapper.addMapper(jobConf, UFO.class, LongWritable.class, Text.class, LongWritable.class,
                Text.class, true, jobConf1);

        JobConf jobConf2 = new JobConf(false);
        ChainMapper.addMapper(jobConf, MapClass.class, LongWritable.class, Text.class,
                Text.class, LongWritable.class, true, jobConf2);

        jobConf.setMapperClass(ChainMapper.class);
        jobConf.setCombinerClass(LongSumReducer.class);
        jobConf.setReducerClass(LongSumReducer.class);

        FileInputFormat.addInputPath(jobConf, new Path(args[0]));
        FileOutputFormat.setOutputPath(jobConf, new Path(args[1]));

        JobClient.runJob(jobConf);
    }
}