package com.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WeekFundDriver {
    public static void main(String[] args) throws Exception {
        // First job: Weekly Fund Flow average calculation
        Configuration conf1 = new Configuration();
        Job job1 = Job.getInstance(conf1, "Weekly Fund Flow average");
        job1.setJarByClass(WeekFundDriver.class);

        job1.setMapperClass(WeekFundFlowMapper.class);
        job1.setReducerClass(WeekFundFlowReducer.class);

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
        Job job2 = Job.getInstance(conf2, "Sort Fund Flow");
        job2.setJarByClass(WeekFundDriver.class);

        job2.setMapperClass(SortMapper.class);
        job2.setReducerClass(SortReducer.class);
        job2.setSortComparatorClass(DescendingLongWritableComparator.class);

        // 设置 Mapper 输出的键和值类型
        job2.setMapOutputKeyClass(LongWritable.class);
        job2.setMapOutputValueClass(Text.class);

        // 设置 Reducer 输出的键和值类型
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job2, intermediateOutput);
        FileOutputFormat.setOutputPath(job2, new Path(args[1]));

        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}
