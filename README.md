# Task 1: 每⽇资⾦流⼊流出统计
$Mapper:$提取`csv`每行第$2$列的`reportDate`，第$5$列的 `total_purchase_amt` 和第$9$列 `total_redeem_amt` 。输出<日期，''流入量，流出量'' >。注意对空白的处理。
```java 
if (totalPurchaseAmt.isEmpty())
	totalPurchaseAmt = "0";
if (totalRedeemAmt.isEmpty())
	totalRedeemAmt = "0";
```
$Reducer:$分别对`purchase_amt`和`redeem_amt`累和。
``` java
long totalInflow = 0;
long totalOutflow = 0;
for (Text value : values) {
	String[] inflowOutflow = value.toString().split(",");
	totalInflow += Long.parseLong(inflowOutflow[0]);
	totalOutflow += Long.parseLong(inflowOutflow[1]);
}
context.write(key, new Text(totalInflow + "," + totalOutflow));
```

# Task 2: 星期交易量统计
思路：先统计星期平均交易量，再降序排列。
**job1:统计星期平均交易量**
$Mapper:$拆解$Task1$的输出<日期，交易量>，将日期转化为weekday。输出<weekday,交易量>
```java
// 处理时间--转化为weekday
DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd");
LocalDate date = LocalDate.parse(reportDate, formatter);
DayOfWeek dayOfWeek = date.getDayOfWeek(); // 获取星期几
String dayName = dayOfWeek.toString().substring(0, 1).toUpperCase()
		+ dayOfWeek.toString().substring(1).toLowerCase(); // 首字母大写，其他字母小写
```
$Reducer:$对每个weekday的交易量累和，然后求平均。
```java
long totalInflow = 0;
long totalOutflow = 0;
long day_num = 0;
for (Text value : values) {
	String[] inflowOutflow = value.toString().split(",");
	totalInflow += Long.parseLong(inflowOutflow[0]);
	totalOutflow += Long.parseLong(inflowOutflow[1]);
	day_num = day_num + 1;
}
context.write(key, new Text(totalInflow/day_num+","+totalOutflow/day_num));	
```
**job2:降序**
$Mapper:$提取流入量。输出<流入量，<weekday,"流入量，流出量">>
$Reducer:$输出$value$。即$job1$的输出形式。
$DescendingLongWritableComparator：$实现降序排序。
```java
@Override
public int compare(WritableComparable a, WritableComparable b) {
	LongWritable lw1 = (LongWritable) a;
	LongWritable lw2 = (LongWritable) b;
	return -lw1.compareTo(lw2); // 反转比较结果以实现降序
}
```

# Task 3:⽤户活跃度分析
思路：先统计用户活跃度，再降序排列。
**job1:统计用户活跃度**
$Mapper:$提取`csv`每行第$1$列的`user_id`，第$6$列的 `directPurchaseAmt` 和第$9$列 `total_redeem_amt` 。输出<用户id ， 活跃度 >。注意对空白的处理。
```java
long active = 0; //active=1，用户活跃；active=0，用户不活跃
// 处理空白值并判断用户是否活跃
if ((!directPurchaseAmt.isEmpty() && !directPurchaseAmt.equals("0")) ||
		(!totalRedeemAmt.isEmpty() && !totalRedeemAmt.equals("0"))) {
	active = 1;
}
```
$Reducer:$对每个用户id的活跃度累和。输出<用户id，活跃天数>
**job2:降序**
$Mapper:$交换$job1$输出的$key$和$value$。输出<活跃天数，用户id>
$Reducer:$交换$key$和$value$。输出<用户id，活跃天数>
$DescendingLongWritableComparator：$实现降序排序。和$Task2$的函数一样。

# Task 4 : 交易⾏为影响因素分析 
思路：对`mfd_bank_shibor.csv`的`Interest_O_N(隔日的利率)`进行统计，计算出每个`weekday`的平均利率，然后和`Task2`中每个`weekday`的平均流出量（交易金额）进行比较。 即研究平均利率和交易的影响。
**job1:统计用户活跃度**
$Mapper:$提取`csv`每行第$1$列的`mfd_date`，第$2$列的 `Interest_O_N` 。将`mdf_date`转化为weekday。输出<weekday,利率>
$Reducer:$对每个`weekday`的利率累和，求平均。输出<`weekday`，平均利率>
**job2:降序**
$Mapper:$交换$job1$输出的$key$和$value$。输出<平均利率，`weekday`>
$Reducer:$交换$key$和$value$。输出<`weekday`，平均利率>
$DescendingLongWritableComparator：$实现降序排序。和$Task2$的函数一样。
**输出结果：**
```java
Saturday        3.6385
Sunday  3.38075
Monday  3.116442105263157
Wednesday       3.103755172413795
Thursday        3.090294736842104
Tuesday 3.0772152542372875
Friday  3.0486263157894733
```
和`Task2`的结果比较（按照资本流出量进行排序）：
```java
Monday  260305810,217463865
Wednesday       254162607,194639446
Tuesday 263582058,191769144
Thursday        236425594,176466674
Friday  199407923,166467960
Sunday  155914551,132427205
Saturday        148088068,112868942
```
**结论：**
当天的利率和资本流出量没有关系。
# 实验结果
运行指令：
```
hadoop jar ./target/lab2-1.0-SNAPSHOT.jar com.example.DailyFundDriver  /hw/lab2/input/user_balance_table.csv  /hw/lab2/output1
hadoop jar ./target/lab2-1.0-SNAPSHOT.jar com.example.WeekFundDriver  /hw/lab2/output1  /hw/lab2/output2
hadoop jar ./target/lab2-1.0-SNAPSHOT.jar com.example.UserActiveDayDriver  /hw/lab2/input/user_balance_table.csv  /hw/lab2/output3
```
结果放在$output$文件夹下

# 遇到的问题与可能的改进
**问题：在运行`Task2`时报错`output`路径已存在。**
通过查询资料发现，当`driver.java`需要处理两个`job`时，如果`job2`失败，在`hdfs`删除`output`文件夹后，还需要删除中间输出`intermediate_output`
```
ubuntu123@std123:/home/lab2$ hadoop fs -rm -r /user/ubuntu123/intermediate_output
```
**改进:**
- 需要加强对$Mapper$、$Reducer$、$Driver$的输入、输出类型检查。
