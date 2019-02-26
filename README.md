# flume
+ 通过tail -f 获取日志文件  
- 现在按照每天一个文件获取，时间定格在00::00切换tail -f 的文件，文件不存在会重试直到存在为止  
* 删除当前文件不报错  
+ flume配置文件配置   
  agent.sources.new_tail.type = NewTailSource//source类  
  agent.sources.new_tail.spolldir=/data1/test//日志文件夹  
  agent.sources.new_tail.new_file_if_same=true//是否每天新生成的文件名字一致（一般log4j每天的日志都是logger.log,昨天的会重命名）    
  agent.sources.new_tail.file_format=logger.{per_day}.log//per_day表示每天一个文件，后台会把{per_day}替换成date  
* 注意事项   
  tail -f 是java代码通过线程开启的子进程，由于代码读取控制台输出的流是阻塞的 所以线程无法打断，只能通过外层时间判断，强行stop线程，导致资源无法释放,所以需要定期手动关闭后台的tail -f进程.这个也是无法避免的问题,所以也可能是flume没有实现递增文件实时读取的原因（flume现在有exe和spooldir）。
