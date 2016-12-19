package org.apache.carbondata.hadoop.api;

import java.io.IOException;

import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DictionaryOutputFormat extends FileOutputFormat<String, Integer> {

  @Override public RecordWriter<String, Integer> getRecordWriter(TaskAttemptContext job)
      throws IOException, InterruptedException {
    return null;
  }
}
