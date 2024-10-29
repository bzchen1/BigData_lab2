package com.example;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class UserActiveDayReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context)
            throws IOException, InterruptedException {
        long total = 0; // 使用基础类型 long 作为累加器

        // 遍历 values 进行累加
        for (LongWritable value : values) {
            total += value.get(); // 使用 .get() 方法获取 LongWritable 的值
        }
        // 输出最终的键值对
        context.write(key, new LongWritable(total));
    }
}
