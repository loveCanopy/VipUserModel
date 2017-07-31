package com.baidumusic.hive;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.RCFile;
import org.apache.hadoop.hive.ql.io.RCFile.Reader;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;/**
 * RCFileRecordReader.
 * 
 * @param <K>
 * @param <V>
 */
public class RCFileRecordReader<K extends LongWritable, V extends BytesRefArrayWritable> extends RecordReader<LongWritable, BytesRefArrayWritable> { 
	private Reader in;
	private long start;
	private long end;
	private boolean more = true;
	private LongWritable key = null;
	private BytesRefArrayWritable value = null;
	protected Configuration conf; /**
	 * Return the progress within the input split.
	 * 
	 * @return 0.0 to 1.0 of the input byte range
	 */
	public float getProgress() throws IOException {
		if (end == start) {
			return 0.0f;
		} else {
			return Math.min(1.0f, (in.getPosition() - start)
					/ (float) (end - start));
		}
	} 
	public void close() throws IOException {
		in.close();
	}

	@Override
	public LongWritable getCurrentKey() throws IOException,
	InterruptedException {  return key;
	} 

	@Override
	public BytesRefArrayWritable getCurrentValue() throws IOException,
	InterruptedException {  return value;
	} 

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		FileSplit fileSplit = (FileSplit) split;
		conf = context.getConfiguration();
		Path path = fileSplit.getPath();
		FileSystem fs = path.getFileSystem(conf);
		this.in = new RCFile.Reader(fs, path, conf);
		this.end = fileSplit.getStart() + fileSplit.getLength();  if (fileSplit.getStart() > in.getPosition()) {
			in.sync(fileSplit.getStart()); // sync to start
		}  this.start = in.getPosition();
		more = start < end;
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if (!more) {
			return false;
		}
		if (key == null) {
			key = new LongWritable();
		}
		if (value == null) {
			value = new BytesRefArrayWritable();
		}
		more = in.next(key);
		if (!more) {
			return false;
		}
		long lastSeenSyncPos = in.lastSeenSyncPos();
		if (lastSeenSyncPos >= end) {
			more = false;
			return more;
		}
		in.getCurrentRow(value);
		return more;
	}
}

