import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

public class UFO extends MapReduceBase implements Mapper<LongWritable, Text, LongWritable, Text> {
    public void map(LongWritable key, Text value, OutputCollector outputCollector, Reporter reporter) throws IOException {
        String line = value.toString();
        if (isValid(line)) {
            outputCollector.collect(key, value);
        }
    }

    public boolean isValid(String line) {
        String[] parts = line.split(",");
        if (parts.length != 11)
            return false;
        return true;
    }
}