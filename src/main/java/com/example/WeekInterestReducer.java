package com.example;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WeekInterestReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        double totalInterestRate = 0.0;
        long dayCount = 0;

        for (Text value : values) {
            try {
                // 解析小数值
                totalInterestRate += Double.parseDouble(value.toString());
                dayCount++;
            } catch (NumberFormatException e) {
                System.err.println("解析错误: " + value.toString());
            }
        }

        if (dayCount > 0) {
            // 计算平均值
            double averageInterestRate = totalInterestRate / dayCount;
            context.write(key, new Text(String.valueOf(averageInterestRate)));
        }
    }
}
