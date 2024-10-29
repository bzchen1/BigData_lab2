package com.example;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//日期转换
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.DayOfWeek;

import java.io.IOException;// 处理与输入和输出操作相关的错误

public class WeekFundFlowMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 拆分出时间、收入开销
        String[] fields = value.toString().split("\t");
        String reportDate = fields[0];
        String totalAmt = fields[1];

        // 处理时间--转化为weekday
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd");
        LocalDate date = LocalDate.parse(reportDate, formatter);
        DayOfWeek dayOfWeek = date.getDayOfWeek(); // 获取星期几
        String dayName = dayOfWeek.toString().substring(0, 1).toUpperCase()
                + dayOfWeek.toString().substring(1).toLowerCase(); // 首字母大写，其他字母小写

        context.write(new Text(dayName), new Text(totalAmt));
    }
}
