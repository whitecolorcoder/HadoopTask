import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

public class Summer
extends Reducer<Text, Text, Text, WordFreq>
{
    public void reduce(Text key, Iterable<Text> values, Context context)
	throws IOException, InterruptedException
	{
        Map<Text, Integer> freqs = new HashMap<>();
        for (Text value : values) {
            freqs.put(value, freqs.getOrDefault(value, 0) + 1);
        }
        WordFreq mostFreq = freqs.entrySet().stream()
                .max(Map.Entry.comparingByValue())
                .map(it -> new WordFreq(key.toString(), it.getKey().toString(), it.getValue()))
                .orElse(new WordFreq(key.toString(), "@isolated word@", 0));

        context.write(key, mostFreq);
    }
}
