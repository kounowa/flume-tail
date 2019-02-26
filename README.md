# flume
+ 通过tail -f 获取日志文件  
+ 现在按照每天一个文件获取，时间定格在00::00切换tail -f 的文件，文件不存在会重试直到存在为止  
+ 删除当前文件不报错  
  agent.sources.new_tail.type = NewTailSource
  agent.sources.new_tail.spolldir=/data1/test
  agent.sources.new_tail.new_file_if_same=true
  agent.sources.new_tail.file_format=logger.{per_day}.log
