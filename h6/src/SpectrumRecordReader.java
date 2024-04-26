import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;

import java.io.*;
import java.util.*;
import java.util.logging.Formatter;
import java.util.regex.*;
import java.util.zip.*;


public class SpectrumRecordReader extends RecordReader<Text, RawSpectrum> {

    // line example: 2012 01 01 00 00   0.00   0.00   0.00   0.00   0.00   0.00   0.00   0.00   ...
    private static final Pattern
            LINE_PATTERN = Pattern.compile("([0-9]{4} [0-9]{2} [0-9]{2} [0-9]{2} [0-9]{2})(.*)");

    private long total_size = 0;
    private long current_size = 0;

    private Text date = null;
    private RawSpectrum rawSpectrum = null;

    private HashMap<String, HashMap<Text, float[]>> vars_data_map;
    private Path[] paths;
    private Iterator<Map.Entry<Text, float[]>> it;

    @Override
    public Text getCurrentKey() {
        return date;
    }

    @Override
    public RawSpectrum getCurrentValue() {
        return rawSpectrum;
    }


    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException {

        CombineFileSplit split = (CombineFileSplit) inputSplit;
        paths = split.getPaths();

        vars_data_map = new HashMap<String, HashMap<Text, float[]>>();

        vars_data_map.put("i", new HashMap<Text, float[]>());
        vars_data_map.put("j", new HashMap<Text, float[]>());
        vars_data_map.put("k", new HashMap<Text, float[]>());
        vars_data_map.put("w", new HashMap<Text, float[]>());
        vars_data_map.put("d", new HashMap<Text, float[]>());

        for (int i = 0; i < paths.length; ++i) {
            FileSystem fileSystem = paths[i].getFileSystem(context.getConfiguration());
            FSDataInputStream inputStream = fileSystem.open(paths[i]);
            GZIPInputStream gzipInputStream = new GZIPInputStream(inputStream);
            LineReader reader = new LineReader(gzipInputStream, context.getConfiguration());

            // Проверяем, подходил ли файл под наш паттерн
            Matcher fileNameMatcher = RawSpectrum.FILENAME_PATTERN.matcher(paths[i].getName());

            if (fileNameMatcher.matches()) {
                Text line = new Text();
                Matcher lineMatcher;
                Text tmpDate;
                String floatsString;
                String variable = fileNameMatcher.group(2);

                while (reader.readLine(line) != 0) {
                    lineMatcher = LINE_PATTERN.matcher(line.toString());
                    if (lineMatcher.matches()) {
                        tmpDate = new Text(lineMatcher.group(1));
                        floatsString = lineMatcher.group(2);
                        ArrayList<Float> floatsArrayList = new ArrayList<Float>();
                        Scanner scanner = new Scanner(floatsString);
                        while (scanner.hasNextFloat()) {
                            floatsArrayList.add(scanner.nextFloat());
                        }

                        float[] floats = new float[floatsArrayList.size()];
                        for (int j = 0; j < floats.length; ++j) {
                            floats[j] = floatsArrayList.get(j);
                        }
                        floatsArrayList.clear();
                        vars_data_map.get(variable).put(tmpDate, floats);
                    }
                }

            }

            reader.close();
            gzipInputStream.close();
            fileSystem.close();
        }


        String min = "i";
        for (String tmp : vars_data_map.keySet()) {
            if (vars_data_map.get(tmp).size() < vars_data_map.get(min).size())
                min = tmp;
        }
        it = vars_data_map.get(min).entrySet().iterator();

        total_size = vars_data_map.get(min).size();

    }


    @Override
    public void close() throws IOException {
    }


    @Override
    public float getProgress() {
        if (total_size == 0)
            return 1f;
        else
            return (float) current_size / (float) total_size;
    }


    @Override
    public boolean nextKeyValue() throws IOException {

        boolean found = false;

        Map.Entry<Text, float[]> entry;

        while (!found && it.hasNext()) {

            entry = entry = (Map.Entry<Text, float[]>) it.next();
            current_size++;
            boolean contains = true;

            if (!vars_data_map.get("i").containsKey(entry.getKey())) contains = false;
            if (!vars_data_map.get("j").containsKey(entry.getKey())) contains = false;
            if (!vars_data_map.get("k").containsKey(entry.getKey())) contains = false;
            if (!vars_data_map.get("w").containsKey(entry.getKey())) contains = false;
            if (!vars_data_map.get("d").containsKey(entry.getKey())) contains = false;


            if (contains) {
                found = true;
                date = new Text(entry.getKey());
                rawSpectrum = new RawSpectrum();
                rawSpectrum.setField("i", vars_data_map.get("i").get(entry.getKey()));
                rawSpectrum.setField("j", vars_data_map.get("j").get(entry.getKey()));
                rawSpectrum.setField("k", vars_data_map.get("k").get(entry.getKey()));
                rawSpectrum.setField("w", vars_data_map.get("w").get(entry.getKey()));
                rawSpectrum.setField("d", vars_data_map.get("d").get(entry.getKey()));
            }
        }

        return found;
    }
}
