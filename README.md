# BDP Practical Questions and Answers
## Simple exam-ready notes for Cloudera / Hadoop environment

> **Note:** These are simplified practical answers based on the questions discussed. Commands may vary with software versions installed in the lab. Question 13 (GraphX PageRank) is intentionally omitted; original numbering for Questions 14 and 15 is preserved.

---

## Question 1: Perform basic HDFS operations

### Aim
To perform basic file operations in Hadoop Distributed File System (HDFS).

### Commands
```bash
start-dfs.sh
start-yarn.sh
jps

echo "Hello Hadoop" > input.txt
hdfs dfs -mkdir -p /user/cloudera/bdp
hdfs dfs -put -f input.txt /user/cloudera/bdp/
hdfs dfs -ls /user/cloudera/bdp
hdfs dfs -cat /user/cloudera/bdp/input.txt
hdfs dfs -get /user/cloudera/bdp/input.txt downloaded.txt
hdfs dfs -rm /user/cloudera/bdp/input.txt
```

### Explanation
- `start-dfs.sh` starts HDFS services.
- `mkdir` creates an HDFS directory.
- `put` copies a local file into HDFS.
- `cat` displays HDFS file contents.
- `get` copies a file from HDFS to the local system.
- `rm` deletes a file from HDFS.

### Expected output
```text
Hello Hadoop
```

---

## Question 2: Import and export data using Sqoop

### Aim
To transfer data between a MySQL database and HDFS using Sqoop.

### Import: MySQL to HDFS
```bash
sqoop import \
--connect jdbc:mysql://localhost/college \
--username root \
--password cloudera \
--table student \
--target-dir /user/cloudera/student \
--num-mappers 1
```

### Export: HDFS to MySQL
First create a destination table named `student_copy` in the `college` database with columns matching the input file. Then run:

```bash
sqoop export \
--connect jdbc:mysql://localhost/college \
--username root \
--password cloudera \
--table student_copy \
--export-dir /user/cloudera/student \
--input-fields-terminated-by ','
```

### Explanation
- **Import** copies rows from MySQL into HDFS.
- **Export** copies records from HDFS into an existing MySQL table.
- Database, table names, credentials, and delimiters must match the lab setup.

### Expected result
The imported data appears under `/user/cloudera/student`; exported rows are inserted into `student_copy`.

---

## Question 3: Collect log events using Flume

### Aim
To collect lines from a log file and send them to HDFS using Apache Flume.

### Step 1: Create a log file
```bash
echo "User logged in" > /home/cloudera/log.txt
```

### Step 2: Create `flume.conf`
```properties
agent.sources = src
agent.channels = ch
agent.sinks = sink

agent.sources.src.type = exec
agent.sources.src.command = tail -F /home/cloudera/log.txt
agent.sources.src.channels = ch

agent.channels.ch.type = memory
agent.channels.ch.capacity = 1000

agent.sinks.sink.type = hdfs
agent.sinks.sink.hdfs.path = hdfs://localhost:8020/user/cloudera/flume
agent.sinks.sink.hdfs.fileType = DataStream
agent.sinks.sink.hdfs.writeFormat = Text
agent.sinks.sink.channel = ch
```

### Step 3: Start Flume
```bash
flume-ng agent \
--conf /etc/flume-ng/conf \
--conf-file flume.conf \
--name agent \
-Dflume.root.logger=INFO,console
```

### Explanation
- **Source** reads log lines.
- **Channel** temporarily stores events.
- **Sink** writes events to HDFS.

### Expected result
The event `User logged in` is written to a file under `/user/cloudera/flume`. Exact configuration keys can differ by Flume version.

---

## Question 4: Implement Word Count using Hadoop MapReduce

### Aim
To count how many times each word occurs in a text file.

### Java program: `WordCount.java`
```java
import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {
  public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      StringTokenizer st = new StringTokenizer(value.toString());
      while (st.hasMoreTokens()) {
        context.write(new Text(st.nextToken()), new IntWritable(1));
      }
    }
  }

  public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable value : values) sum += value.get();
      context.write(key, new IntWritable(sum));
    }
  }

  public static void main(String[] args) throws Exception {
    Job job = Job.getInstance(new Configuration(), "word count");
    job.setJarByClass(WordCount.class);
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
```

### Compile and run
```bash
hadoop com.sun.tools.javac.Main WordCount.java
jar cf wc.jar WordCount*.class

echo "cat dog cat" > input.txt
hdfs dfs -put -f input.txt /user/cloudera/input.txt
hadoop jar wc.jar WordCount /user/cloudera/input.txt /user/cloudera/wcout
hdfs dfs -cat /user/cloudera/wcout/part-r-00000
```

If you run it again, remove the old output directory first:
```bash
hdfs dfs -rm -r /user/cloudera/wcout
```

### Explanation
- **Mapper:** emits each word with the value `1`.
- **Reducer:** adds the values for each word.
- Hadoop writes the final counts to the output directory.

### Expected output
```text
cat    2
dog    1
```

---

## Question 5: Perform Hive database and table operations

### Aim
To create and manage databases and tables using Hive.

### Commands
```sql
CREATE DATABASE student;
SHOW DATABASES;
USE student;
SHOW TABLES;
```

To remove the database and its tables:
```sql
DROP DATABASE student CASCADE;
```

### Explanation
- `CREATE DATABASE` creates a database.
- `USE` selects the database.
- `SHOW TABLES` lists its tables.
- `DROP DATABASE ... CASCADE` removes the database and its contents.

### Expected result
The `student` database appears in `SHOW DATABASES` and is removed by the final command.

---

## Question 6: Analyze sales data using Hive

### Aim
To create sales tables and perform aggregate queries.

### Create database and tables
```sql
CREATE DATABASE sales;
USE sales;

CREATE TABLE product (
  id INT, name STRING, category STRING, price DOUBLE
);

CREATE TABLE customer (
  id INT, name STRING
);

CREATE TABLE sales_order (
  id INT, customer_id INT, product_id INT,
  order_date STRING, quantity INT, total_amount DOUBLE
);
```

### Insert sample records
```sql
INSERT INTO product VALUES
(1,'Phone','Electronics',10000.0),
(2,'Book','Education',200.0),
(3,'Laptop','Electronics',50000.0);

INSERT INTO customer VALUES
(1,'Ravi'),(2,'Ram');

INSERT INTO sales_order VALUES
(1,1,1,'2026-01-10',2,20000.0),
(2,2,2,'2026-02-12',3,600.0),
(3,1,3,'2026-02-15',1,50000.0);
```

### Query 1: Total sales
```sql
SELECT SUM(total_amount) AS total_sales FROM sales_order;
```
**Expected result:** `70600.0`

### Query 2: Quantity sold for each product
```sql
SELECT product_id, SUM(quantity) AS total_quantity
FROM sales_order
GROUP BY product_id
ORDER BY total_quantity DESC;
```
**Expected result:**
```text
2    3
1    2
3    1
```

### Query 3: Total spending by each customer
```sql
SELECT customer_id, SUM(total_amount) AS total_spending
FROM sales_order
GROUP BY customer_id;
```
**Expected result:** customer 1 = `70000.0`; customer 2 = `600.0`.

### Query 4: Monthly sales
```sql
SELECT SUBSTR(order_date, 1, 7) AS month,
       SUM(total_amount) AS monthly_sales
FROM sales_order
GROUP BY SUBSTR(order_date, 1, 7)
ORDER BY month;
```
**Expected result:** January 2026 = `20000.0`; February 2026 = `50600.0`.

### Query 5: Maximum product price by category
```sql
SELECT category, MAX(price) AS maximum_price
FROM product
GROUP BY category;
```
**Expected result:** Electronics = `50000.0`; Education = `200.0`.

### Explanation
Hive SQL is used to group records and calculate totals, averages, and maximum values.

---

## Question 7: Create and use a simple Hive UDF

### Aim
To create a User-Defined Function (UDF) that converts text to uppercase.

### Java program: `Upper.java`
```java
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public class Upper extends UDF {
  public Text evaluate(Text s) {
    return s == null ? null : new Text(s.toString().toUpperCase());
  }
}
```

### Compile and package
```bash
javac -cp "$(hadoop classpath):$(hive --service classpath)" Upper.java
jar cf upper.jar Upper.class
```

### Run in Hive
Use the actual path where `upper.jar` was saved:
```sql
ADD JAR /home/cloudera/upper.jar;
CREATE TEMPORARY FUNCTION myupper AS 'Upper';
SELECT myupper('hello');
```

### Expected output
```text
HELLO
```

### Explanation
A UDF adds a custom function to Hive. Here, `myupper()` converts a string to uppercase.

---

## Question 8: Integrate Hive with HBase

### Aim
To access HBase data through a Hive external table.

### Step 1: Create an HBase table
Open the HBase shell:
```text
create 'employee_hbase', 'personal', 'professional'
```

### Step 2: Create a Hive external table
Run in Hive, assuming the HBase storage handler is installed and compatible:
```sql
CREATE EXTERNAL TABLE employee_hive (
  rowkey STRING,
  name STRING,
  age INT,
  designation STRING
)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES (
  'hbase.columns.mapping' = ':key,personal:name,personal:age,professional:designation'
)
TBLPROPERTIES ('hbase.table.name' = 'employee_hbase');
```

### Step 3: Insert and view records
```sql
INSERT INTO TABLE employee_hive
VALUES ('1','Ravi',22,'Developer'), ('2','Ram',23,'Analyst');

SELECT * FROM employee_hive;
```

### Explanation
HBase stores data in column families. Hive provides a SQL-style interface to query the mapped HBase table. This requires compatible Hive-HBase integration libraries and configuration.

---

## Question 9: Perform CRUD operations in HBase

### Aim
To create a table and insert, retrieve, scan, and delete employee data.

### Commands
Open the HBase shell:
```text
create 'employee', 'personal', 'professional'

put 'employee', '1', 'personal:name', 'Ravi'
put 'employee', '1', 'personal:age', '22'
put 'employee', '1', 'professional:designation', 'Developer'

put 'employee', '2', 'personal:name', 'Ram'
put 'employee', '2', 'personal:age', '23'

scan 'employee'
get 'employee', '1'

delete 'employee', '1', 'personal:age'
get 'employee', '1'
```

### Explanation
- `create` creates a table and column families.
- `put` inserts or updates a cell.
- `scan` displays rows in the table.
- `get` displays one row.
- `delete` removes a cell.

### Expected result
The first `get` displays Ravi's name, age, and designation. After `delete`, the age cell is absent.

---

## Question 10: Perform basic operations using PySpark RDDs

### Aim
To count employee records, filter employees, extract names, and calculate average salary by department.

### Python program
```python
from pyspark import SparkContext

sc = SparkContext.getOrCreate()

data = [
    ("Ravi", "IT", 50000, 2016),
    ("Ram", "HR", 40000, 2013),
    ("Sita", "IT", 60000, 2018),
    ("John", "HR", 30000, 2017)
]

rdd = sc.parallelize(data)

print("Count:", rdd.count())
print("Joined after 2014:", rdd.filter(lambda x: x[3] > 2014).collect())
print("Names:", rdd.map(lambda x: x[0]).collect())

totals = rdd.map(lambda x: (x[1], (x[2], 1))).reduceByKey(
    lambda a, b: (a[0] + b[0], a[1] + b[1])
)

print("Average salary:",
      sorted(totals.mapValues(lambda x: x[0] / x[1]).collect()))

sc.stop()
```

### Expected output
```text
Count: 4
Joined after 2014: [('Ravi', 'IT', 50000, 2016), ('Sita', 'IT', 60000, 2018), ('John', 'HR', 30000, 2017)]
Names: ['Ravi', 'Ram', 'Sita', 'John']
Average salary: [('HR', 35000.0), ('IT', 55000.0)]
```

### Explanation
An RDD is a distributed collection. `filter` selects records, `map` transforms them, and `reduceByKey` combines values for each department.

---

## Question 11: Perform employee analysis using a PySpark DataFrame

### Aim
To calculate average department salary, employee experience, and salary category.

### Python program
```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, when, col
from pyspark.sql.window import Window

spark = SparkSession.builder.getOrCreate()

data = [
    ("Ravi", "IT", 50000, 2016),
    ("Ram", "HR", 40000, 2013),
    ("Sita", "IT", 60000, 2018),
    ("John", "HR", 30000, 2017)
]

df = spark.createDataFrame(data, ["name", "dept", "salary", "join_year"])

window = Window.partitionBy("dept")
result = df.withColumn("department_average", avg("salary").over(window))
result = result.withColumn("experience", 2026 - col("join_year"))
result = result.withColumn(
    "salary_category",
    when(col("salary") >= 50000, "High").otherwise("Low")
)

result.select(
    "name", "dept", "salary", "department_average",
    "experience", "salary_category"
).show()

spark.stop()
```

### Expected result (key values)
| Name | Department average | Experience | Category |
|---|---:|---:|---|
| Ravi | 55000 | 10 | High |
| Ram | 35000 | 13 | Low |
| Sita | 55000 | 8 | High |
| John | 35000 | 9 | Low |

### Explanation
A DataFrame stores structured rows. A window function calculates each department's average without collapsing employee rows. Experience is calculated using `2026 - join_year`.

---

## Question 12: Handle missing values in a PySpark DataFrame

### Aim
To identify and replace missing salary values and save the output.

### Python program
```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.getOrCreate()

data = [
    ("Ravi", 50000),
    ("Ram", None),
    ("Sita", 60000)
]

df = spark.createDataFrame(data, ["name", "salary"])

df.printSchema()
df.show()

print("Rows with missing salary:")
df.filter(col("salary").isNull()).show()

print("After replacing missing salary with 0:")
clean_df = df.fillna({"salary": 0})
clean_df.show()

clean_df.write.mode("overwrite").option("header", True).csv(
    "/tmp/employee_output"
)

spark.stop()
```

### Expected result
Before filling, Ram's salary is `null`. After filling, Ram's salary is `0`. The output is saved to `/tmp/employee_output`.

### Explanation
`isNull()` finds missing values. `fillna()` replaces them. Spark writes CSV output as a directory containing one or more part files.

---

## Question 14: Perform employee analytics using HBase data

### Aim
To filter employees by salary and calculate average salary by department using PySpark.

> **Important:** This is a simple analytics demonstration using sample data. It does not directly read from HBase.

### Python program
```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg

spark = SparkSession.builder.getOrCreate()

data = [
    ("Ravi", "IT", 50000),
    ("Ram", "HR", 40000),
    ("Sita", "IT", 60000)
]

df = spark.createDataFrame(data, ["name", "dept", "salary"])

print("Employees earning more than 40000:")
df.filter(df.salary > 40000).show()

print("Average salary by department:")
df.groupBy("dept").agg(
    avg("salary").alias("average_salary")
).show()

spark.stop()
```

### Expected output (key values)
Employees earning more than `40000`: Ravi and Sita.

Average salary:
```text
HR    40000.0
IT    55000.0
```

### Explanation
`filter()` selects employees earning more than 40000. `groupBy()` groups employees by department, and `avg()` calculates the average salary.

---

## Question 15: Set up Kafka locally, create topics, and implement an event-handling system with subscribers

### Aim
To create a Kafka topic, publish events, and receive them through two independent subscribers.

### Step 1: Start ZooKeeper
In Terminal 1, from the Kafka installation directory:
```bash
bin/zookeeper-server-start.sh config/zookeeper.properties
```

### Step 2: Start Kafka
In Terminal 2:
```bash
bin/kafka-server-start.sh config/server.properties
```

### Step 3: Create a topic
In Terminal 3:
```bash
bin/kafka-topics.sh --create \
--zookeeper localhost:2181 \
--replication-factor 1 \
--partitions 1 \
--topic events
```

Check the topic:
```bash
bin/kafka-topics.sh --list --zookeeper localhost:2181
```

### Step 4: Start subscriber 1
In Terminal 4:
```bash
bin/kafka-console-consumer.sh \
--zookeeper localhost:2181 \
--topic events \
--group subscriber1 \
--from-beginning
```

### Step 5: Start subscriber 2
In Terminal 5:
```bash
bin/kafka-console-consumer.sh \
--zookeeper localhost:2181 \
--topic events \
--group subscriber2 \
--from-beginning
```

### Step 6: Start the producer
In Terminal 6:
```bash
bin/kafka-console-producer.sh \
--broker-list localhost:9092 \
--topic events
```

Type these messages, pressing Enter after each:
```text
Student registered
Order received
Payment completed
```

### Expected result
Both subscriber terminals should receive:
```text
Student registered
Order received
Payment completed
```

### Explanation
- **Producer:** publishes messages to a topic.
- **Topic:** named stream of events; here it is `events`.
- **Consumer/subscriber:** reads messages from the topic.
- **Consumer group:** subscribers in different groups each receive their own copy of the event stream. That is why `subscriber1` and `subscriber2` can both receive every message.

### Important compatibility note
These commands use older ZooKeeper-based Kafka syntax often found in older lab installations. Newer Kafka versions may use different options or KRaft mode. Use commands that match the Kafka version installed in the lab.

---

## Quick revision: Important terms

| Term | Simple meaning |
|---|---|
| HDFS | Stores files across a Hadoop cluster |
| MapReduce | Processes large data using Map and Reduce stages |
| Sqoop | Transfers data between relational databases and Hadoop |
| Flume | Collects and moves log/event data |
| Hive | SQL-like querying for data in Hadoop |
| UDF | User-Defined Function |
| HBase | Distributed NoSQL database |
| RDD | Distributed collection in Spark |
| DataFrame | Structured data with named columns |
| Kafka producer | Sends events |
| Kafka topic | Named stream of events |
| Kafka consumer | Reads events |
