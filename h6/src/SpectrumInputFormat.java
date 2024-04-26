import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SpectrumInputFormat
extends InputFormat<Text,RawSpectrum>
{

	@Override
	public List<InputSplit>
	getSplits(JobContext context)
	throws IOException, InterruptedException
	{

		HashMap<String, ArrayList<FileStatus>> dev_id_path = new HashMap();

		for (Path path : FileInputFormat.getInputPaths(context)) {
			FileSystem fs = path.getFileSystem(context.getConfiguration());
			for (FileStatus file : fs.listStatus(path)) {
				String dev_id = file.getPath().getName().toString().substring(0,5);
				if (!dev_id_path.containsKey(dev_id)) {
					ArrayList<FileStatus> dev_files = new ArrayList<FileStatus>();
					dev_files.add(file);
					dev_id_path.put(dev_id, dev_files);
				} else {
					dev_id_path.get(dev_id).add(file);
				}
			}
		}

		ArrayList<InputSplit> combine_file_splits = new ArrayList<>();
		for (Map.Entry<String, ArrayList<FileStatus>> oneSplit : dev_id_path.entrySet()) {
			int size = oneSplit.getValue().size();

			if (size == 5 ){
				Path[] paths = new Path[5];
				long[] file_sizes = new long[5];

				FileStatus[] file_statuses = new FileStatus[5];
				file_statuses = oneSplit.getValue().toArray(file_statuses);
				for (int i=0; i<5; ++i) {
					paths[i] = file_statuses[i].getPath();
					file_sizes[i] = file_statuses[i].getLen();
				}
				combine_file_splits.add(new CombineFileSplit(paths, file_sizes));
			}

		}

		return combine_file_splits;
	}

	@Override
	public RecordReader<Text,RawSpectrum>
	createRecordReader(InputSplit split, TaskAttemptContext context)
	throws IOException, InterruptedException
	{
		return new SpectrumRecordReader();
	}

}
