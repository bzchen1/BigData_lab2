package com.example;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SortMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 读取每一行
        String line = value.toString();
        // 按照空格分割
        String[] parts = line.split("\\s+");
        if (parts.length == 2) {
            // 获取天数
            String day = parts[0];
            // 获取逗号分割的第二列的第一个值
            String[] numbers = parts[1].split(",");
            long mainValue = Long.parseLong(numbers[0]);

            // 输出以 mainValue 为 key，day 为 value 的键值对
            context.write(new LongWritable(mainValue), new Text(day + "\t" + parts[1]));
        }
    }
}
