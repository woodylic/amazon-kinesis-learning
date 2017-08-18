### 项目说明

根据AWS文档的实践代码：

http://docs.aws.amazon.com/streams/latest/dev/learning-kinesis-module-one.html

folk自

https://github.com/awslabs/amazon-kinesis-learning/tree/learning-module-1

修改为maven项目，添加了依赖，修改了编译不过的老代码，以方便测试。

### 代码说明

**Producer:**

/writer/StockTradesWriter，用于持续向StockTradeStream发送模拟股票交易信息。

**Consumer:**

/processor/StockTradesProcessor，用于持续处理StockTradesWriter创建的股票交易流，并输出每分钟买入和卖出最多的股票。

### 使用说明

1. 根据AWS文档创建好Stream，配置好用户。
2. 下载源码。
3. mvn build。
4. 根据所在region和建立的stream，修改StockTradesWriter和StockTradesProcessor的main函数里面的regionName和streamName参数。
5. 启动StockTradesWriter。
6. 启动StockTradesProcessor。