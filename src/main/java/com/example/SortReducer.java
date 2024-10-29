package com.example;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SortReducer extends Reducer<LongWritable, Text, Text, Text> {

    @Override
    protected void reduce(LongWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        for (Text value : values) {
            // value 格式：天数 + 逗号分割的数字
            String[] parts = value.toString().split("\t");
            if (parts.length == 2) {
                String day = parts[0]; // 天数
                String rest = parts[1]; // 逗号分隔的数字
                // 输出格式：天数，后续数字
                context.write(new Text(day), new Text(rest));
            }
        }
    }
}
