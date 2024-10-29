package com.example;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SortActiveMapper extends Mapper<LongWritable, Text, DoubleWritable, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 读取每一行
        String line = value.toString();
        // 按照空格分割
        String[] parts = line.split("\\s+");

        if (parts.length == 2) {
            try {
                // 获取 ID 和活跃天数
                String userId = parts[0];
                double activeDay = Double.parseDouble(parts[1]);

                // 输出以 activeDay 为 key，userId 为 value
                context.write(new DoubleWritable(activeDay), new Text(userId));
            } catch (NumberFormatException e) {
                System.err.println("解析错误: " + parts[1]);
            }
        } else {
            System.err.println("无效行: " + line);
        }
    }
}
