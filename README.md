# flume
1.通过tail -f 获取日志文件
2.现在按照每天一个文件获取，时间定格在00::00切换tail -f 的文件，文件不存在会重试直到存在为止
3.删除当前文件不报错
