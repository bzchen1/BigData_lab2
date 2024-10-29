package com.example;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;// 处理与输入和输出操作相关的错误

//日期转换
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.DayOfWeek;

public class WeekInterestMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 拆分出每一列
        String[] fields = value.toString().split(",");

        // 排除第一列
        if (fields[0].equals("mfd_date"))
            return;
        String mfdDate = fields[0];
        String Interest_O_N = fields[1];

        // 处理时间--转化为weekday
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd");
        LocalDate date = LocalDate.parse(mfdDate, formatter);
        DayOfWeek dayOfWeek = date.getDayOfWeek(); // 获取星期几
        String dayName = dayOfWeek.toString().substring(0, 1).toUpperCase()
                + dayOfWeek.toString().substring(1).toLowerCase(); // 首字母大写，其他字母小写

        context.write(new Text(dayName), new Text(Interest_O_N));
    }
}
