package com.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.DoubleWritable;

public class WeekInterestDriver {
    public static void main(String[] args) throws Exception {
        // job1:统计weekday的平均收益率
        Configuration conf1 = new Configuration();
        Job job1 = Job.getInstance(conf1, "Weekday interest");
        job1.setJarByClass(WeekInterestDriver.class);

        job1.setMapperClass(WeekInterestMapper.class);
        job1.setReducerClass(WeekInterestReducer.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job1, new Path(args[0]));
        Path intermediateOutput = new Path("intermediate_output");
        FileOutputFormat.setOutputPath(job1, intermediateOutput);

        if (!job1.waitForCompletion(true)) {
            System.exit(1);
        }
        // job2:降序排序

        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "Sort weekday interest");
        job2.setJarByClass(WeekInterestDriver.class);

        job2.setMapperClass(SortActiveMapper.class);
        job2.setReducerClass(SortActiveReducer.class);
        job2.setSortComparatorClass(DescendingLongWritableComparator.class);

        // 设置 Mapper 输出的键和值类型
        job2.setMapOutputKeyClass(DoubleWritable.class);
        job2.setMapOutputValueClass(Text.class);

        // 设置 Reducer 输出的键和值类型
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(job2, intermediateOutput);
        FileOutputFormat.setOutputPath(job2, new Path(args[1]));

        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}
