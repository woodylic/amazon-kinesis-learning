根据AWS文档的实践代码：
http://docs.aws.amazon.com/streams/latest/dev/learning-kinesis-module-one.html

folk自https://github.com/awslabs/amazon-kinesis-learning/tree/learning-module-1，并修改为maven项目。

**Producer:**

/writer/StockTradesWriter，用于持续向StockTradeStream发送模拟股票交易信息。

**Consumer:**

/processor/StockTradesProcessor，用于持续处理StockTradesWriter创建的股票交易流，并输出每分钟买入和卖出最多的股票。