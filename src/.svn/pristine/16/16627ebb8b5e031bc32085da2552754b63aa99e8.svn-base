package com.baidumusic.hive;

import java.io.IOException;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/**
 * RCFileInputFormat.
 *
 * @param <K>
 * @param <V>
 */
public class RCFileInputFormat<K extends LongWritable, V extends BytesRefArrayWritable> extends FileInputFormat<K, V>  {

	public RCFileInputFormat() {
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public org.apache.hadoop.mapreduce.RecordReader<K, V> createRecordReader(
			org.apache.hadoop.mapreduce.InputSplit arg0, TaskAttemptContext arg1)
					throws IOException, InterruptedException {
		return new RCFileRecordReader();
		
	}
}
