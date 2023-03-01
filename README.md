# self-healing
1.使用python3.11写的一个故障自愈程序。
2.本程序在window2019 server运行测试通过
3.通过监控java程序里的详细启动包名来判断进程是否存在
4.判断kafka相应topic里有没有写入数据，因为性能原因采用的是最新偏移量求和。来判断是否有数据写入。
5.根据指定的时间段内进行单独进程判断或kafka写入数据判断
6.日志打印分二级INFO与ERROR，方便调试与日志监控报警
7.程序内置钉钉报警

安装注意事项：
1.安装时要注意pip install kafka 然后是pip install kafka-python,
2.其它按提示安装及可。
