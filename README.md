# Real-Time Alerting System Using Flink - Table API and SQL


## Working

This is an example of a real-time alerting system. 

1.) We create 2 CSV files, locally for the example (one as Event Stream and the other one as Pattern Stream)

2.) Then we create two Kafka streams with 2 different topics namely `broadcast` (for Event Data Stream) and `pattern` ( for pattern stream)

3.) Then we start by creating 2 environments in flink(one for table and one for the stream)

4.) After this streams are mapped accordingly so as to change them into Dynamic Table.

5.)Join(continuous/SQL queries) are performed upon the table/s so as to get the desired result.


## How to Run the Code

Event Stream with topic -- broadcast and table name as

```
1,"A",35
2,"B",45
3,"C",55
4,"D",69
5,"E",555
```

Pattern Stream with topic -- pattern and table name as 

```
1,"A", 50
2,"B", 69
3,"C", 86
4,"D", 9.6
5,"E", 55
```

Turn these into Kafka stream run the code, then run these csv files.

