import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

public class Summer extends Reducer<Text, Text, Text, Text> {
    public void reduce(Text key, Iterable<Text> values, Context context)
    throws IOException, InterruptedException
    {
        Map<Text, Integer> freqs = new HashMap<>();
        for (Text value : values) {
            freqs.put(value, freqs.getOrDefault(value, 0) + 1);
        }
        Text mostFreq = freqs.entrySet().stream()
                .max(Map.Entry.comparingByValue())
                .map(Map.Entry::getKey)
                .orElse(new Text("@isolated word@"));

        context.write(key, mostFreq);
    }
}
