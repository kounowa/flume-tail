/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Charsets;
import org.apache.flume.*;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.conf.BatchSizeSupported;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.instrumentation.SourceCounter;
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.nio.charset.Charset;

/**
 * 新版本的tail -f 文件（新设立文件夹用于获取每日新增的文件内容log4j生成的日志）（实时获取)
 * log4j生成类似logger.2018-10-19.log
 * 貌似exex启动
 */
public class NewTailSource extends AbstractSource implements EventDrivenSource, Configurable,
        BatchSizeSupported {

    private static final Logger logger = LoggerFactory.getLogger(NewTailSource.class);

    //目标dir(绝对路径)
    private String spolldir;
    //新增文件是否名字一致
    private boolean new_file_if_same;
    //文件格式
    private String file_format;
    private String shell;
    private String command;
    private SourceCounter sourceCounter;
//    private ExecutorService executor;
    private Thread th;
    private Future<?> runnerFuture;
    private long restartThrottle;
    private boolean restart;
    private boolean logStderr;
    private Integer bufferCount;
    private long batchTimeout;
    private ExecRunnable runner;
    private Charset charset;
    private NewTailSourceConfigurationConstants.Time_Type ttpy;
    private String lastPushDate = ExecRunnable.now_date();
    private Boolean if_started = false;

    private void check_dir_if_access(){
        try {
            File canary = File.createTempFile("flume-spooldir-perm-check-", ".canary",
                    new File( spolldir ));
            Files.write(canary.toPath(), "testing flume file permissions\n".getBytes());
            List<String> lines = Files.readAllLines(canary.toPath(), Charsets.UTF_8);
            Preconditions.checkState(!lines.isEmpty(), "Empty canary file %s", canary);
            if (!canary.delete()) {
                throw new IOException("Unable to delete canary file " + canary);
            }
            logger.debug("Successfully created and deleted canary file: {}", canary);
        } catch (IOException e) {
            throw new FlumeException("Unable to read and modify files" +
                    " in the spooling directory: " + spolldir, e);
        }
    }

    @Override
    public void start() {
        // Mark the Source as RUNNING.
        super.start();

        logger.info("Exec source starting with command: {}", command);
        //测试文件夹是否可以连接
        check_dir_if_access();

        // Start the counter before starting any threads that may access it.
        sourceCounter.start();

        while (true){
            if (lastPushDate.compareTo(ExecRunnable.now_date())<0){
                //如果已经有启动的则直接关闭,重新生成一个
                if (if_started){
                    th.stop();
                    logger.info( "停止"+lastPushDate+"的线程,貌似风险很大" );
                }
                runner = new ExecRunnable( spolldir, shell, command, getChannelProcessor(), sourceCounter, restart,
                        restartThrottle, logStderr, bufferCount, batchTimeout, charset, new_file_if_same, file_format, ttpy );

                // Start the runner thread.
//                runnerFuture = executor.submit( runner );
                //不用线程池，线程池没法stop
                th = new Thread( runner );
                th.start();
                if_started = true;
                // Mark the Source as RUNNING.
                super.start();
                logger.info( "新的一天开始了 重启代码" );
                lastPushDate = ExecRunnable.now_date();

            }else{
                if (if_started){
                    try {
                        Thread.sleep( 10000L );
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }else{
//                    executor = Executors.newSingleThreadExecutor();
                    runner = new ExecRunnable( spolldir, shell, command, getChannelProcessor(), sourceCounter, restart,
                            restartThrottle, logStderr, bufferCount, batchTimeout, charset, new_file_if_same, file_format, ttpy );

                    // Start the runner thread.
//                    runnerFuture = executor.submit( runner );
                    th = new Thread( runner );
                    th.start();
                    if_started = true;
                    // Mark the Source as RUNNING.
                    super.start();

                }
            }
        }

    }

    @Override
    public void stop() {
        logger.info("Stopping exec source with command: {}", command);
        if (runner != null) {
            runner.setRestart(false);
            runner.kill();
        }

        if (runnerFuture != null) {
            logger.debug("Stopping exec runner");
            runnerFuture.cancel(true);
            logger.debug("Exec runner stopped");
        }
//        executor.shutdown();
//
//        while (!executor.isTerminated()) {
//            logger.debug("Waiting for exec executor service to stop");
//            try {
//                executor.awaitTermination(500, TimeUnit.MILLISECONDS);
//            } catch (InterruptedException e) {
//                logger.debug("Interrupted while waiting for exec executor service "
//                        + "to stop. Just exiting.");
//                Thread.currentThread().interrupt();
//            }
//        }

        sourceCounter.stop();
        super.stop();

        logger.debug("Exec source with command:{} stopped. Metrics:{}", command,
                sourceCounter);
    }

    @Override
    public void configure(Context context){
        command = context.getString("command");
//        Preconditions.checkState(command != null,
//                "The parameter command must be specified");
        spolldir = context.getString(NewTailSourceConfigurationConstants.SPOLL_DIR);
        //这个目录一定要存在不存在爆炸
        Preconditions.checkState(spolldir != null,
                "The parameter spolldir must be specified");

        restartThrottle = context.getLong(NewTailSourceConfigurationConstants.CONFIG_RESTART_THROTTLE,
                NewTailSourceConfigurationConstants.DEFAULT_RESTART_THROTTLE);

        restart = context.getBoolean(NewTailSourceConfigurationConstants.CONFIG_RESTART,
                NewTailSourceConfigurationConstants.DEFAULT_RESTART);

        logStderr = context.getBoolean(NewTailSourceConfigurationConstants.CONFIG_LOG_STDERR,
                NewTailSourceConfigurationConstants.DEFAULT_LOG_STDERR);

        bufferCount = context.getInteger(NewTailSourceConfigurationConstants.CONFIG_BATCH_SIZE,
                NewTailSourceConfigurationConstants.DEFAULT_BATCH_SIZE);

        batchTimeout = context.getLong(NewTailSourceConfigurationConstants.CONFIG_BATCH_TIME_OUT,
                NewTailSourceConfigurationConstants.DEFAULT_BATCH_TIME_OUT);

        charset = Charset.forName(context.getString(NewTailSourceConfigurationConstants.CHARSET,
                NewTailSourceConfigurationConstants.DEFAULT_CHARSET));

        shell = context.getString(NewTailSourceConfigurationConstants.CONFIG_SHELL, null);

        if (sourceCounter == null) {
            sourceCounter = new SourceCounter(getName());
        }
        //日志文件是否每天都一样 类似logger.log
        new_file_if_same = context.getBoolean(NewTailSourceConfigurationConstants.NEW_FILE_IF_SAME,NewTailSourceConfigurationConstants.DEFAULT_NEW_FILE_IF_SAME);

        //日志文件的格式一定是需要的
        file_format = context.getString(NewTailSourceConfigurationConstants.FILE_FORMAT);
        Preconditions.checkState(file_format != null,
                "The parameter spolldir must be specified");
        //根据file_format获取日志的时间间隔
        String temp_str = file_format.substring( file_format.indexOf( "{" )+1,file_format.indexOf( "}" ) );
        try{
            ttpy = NewTailSourceConfigurationConstants.Time_Type.valueOf( temp_str );
        }catch (Exception e){
            throw new FlumeException("参数file_format配置不正确" + temp_str, e);
        }
    }

    @Override
    public long getBatchSize() {
        return bufferCount;
    }

    private static class ExecRunnable implements Runnable {

        public ExecRunnable(String spooldir,String shell, String command, ChannelProcessor channelProcessor,
                            SourceCounter sourceCounter, boolean restart, long restartThrottle,
                            boolean logStderr, int bufferCount, long batchTimeout, Charset charset,
                            boolean new_file_if_same,String file_format,NewTailSourceConfigurationConstants.Time_Type ttpy) {
            this.spooldir = spooldir;
            this.command = command;
            this.channelProcessor = channelProcessor;
            this.sourceCounter = sourceCounter;
            this.restartThrottle = restartThrottle;
            this.bufferCount = bufferCount;
            this.batchTimeout = batchTimeout;
            this.restart = restart;
            this.logStderr = logStderr;
            this.charset = charset;
            this.shell = shell;
            this.new_file_if_same = new_file_if_same;
            this.file_format = file_format;
            this.ttpy = ttpy;
        }

        private final String spooldir;
        private final String shell;
        private final String command;
        private final ChannelProcessor channelProcessor;
        private final SourceCounter sourceCounter;
        private volatile boolean restart;
        private final long restartThrottle;
        private final int bufferCount;
        private long batchTimeout;
        private final boolean logStderr;
        private final Charset charset;
        private boolean new_file_if_same;
        private String file_format;
        private NewTailSourceConfigurationConstants.Time_Type ttpy;
        private Process process = null;
        private SystemClock systemClock = new SystemClock();
        private Long lastPushToChannel = systemClock.currentTimeMillis();

        ScheduledExecutorService timedFlushService;
        ScheduledFuture<?> future;

        private static String now_date(){
            Date nowTime = new Date(System.currentTimeMillis());
            SimpleDateFormat sdFormatter = new SimpleDateFormat("yyyy-MM-dd");
            String retStrFormatNowDate = sdFormatter.format(nowTime);
            return retStrFormatNowDate;
        }

        @Override
        public void run() {
            do {

                String final_file = "";
                switch (ttpy){
                    case per_day:
                        if (new_file_if_same == true) {
                            //担心多带了个点
                            final_file = file_format.replace( "{" + ttpy.name() + "}.", "" );
                            final_file = final_file.replace( "{" + ttpy.name() + "}", "" );
                        }else{
                            final_file = file_format.replace( "{" + ttpy.name() + "}", now_date() );
                        }

                        break;
                    default:
                        break;
                }
                final_file = spooldir+"/"+final_file;
                logger.info( "文件"+final_file+"开始处理" );
                String exitCode = "unknown";
                BufferedReader reader = null;
                String line = null;
                final List<Event> eventList = new ArrayList<Event>();

                timedFlushService = Executors.newSingleThreadScheduledExecutor(
                        new ThreadFactoryBuilder().setNameFormat(
                                "timedFlushExecService" +
                                        Thread.currentThread().getId() + "-%d").build());
                try {
                    if (shell != null) {
                        String[] commandArgs = formulateShellCommand(shell, command);
                        process = Runtime.getRuntime().exec(commandArgs);
                    }  else {
                        String[] commandArgs = ("tail -f "+final_file).split("\\s+");
                        process = new ProcessBuilder(commandArgs).start();
                    }
                    reader = new BufferedReader(
                            new InputStreamReader(process.getInputStream(), charset));

                    // StderrLogger dies as soon as the input stream is invalid
                    StderrReader stderrReader = new StderrReader(new BufferedReader(
                            new InputStreamReader(process.getErrorStream(), charset)), logStderr);
                    stderrReader.setName("StderrReader-[" + command + "]");
                    stderrReader.setDaemon(true);
                    stderrReader.start();

                    future = timedFlushService.scheduleWithFixedDelay(new Runnable() {
                      @Override
                      public void run() {
                          try {
                              synchronized (eventList) {
                                  if (!eventList.isEmpty() && timeout()) {
                                      flushEventBatch(eventList);
                                  }
                              }
                          } catch (Exception e) {
                              logger.error("Exception occurred when processing event batch", e);
                              if (e instanceof InterruptedException) {
                                  Thread.currentThread().interrupt();
                              }
                          }
                      }
                                                                      },
                            batchTimeout, batchTimeout, TimeUnit.MILLISECONDS);
                    //代码将会一直卡在这 除非外部stop
                    while ((line = reader.readLine()) != null) {
                        sourceCounter.incrementEventReceivedCount();
                        synchronized (eventList) {
                            eventList.add(EventBuilder.withBody(line.getBytes(charset)));
                            //batch大一count或者超过了时间就推送（有可能还有数据没推送，但是已经结束了循环)
                            if (eventList.size() >= bufferCount || timeout()) {
                                flushEventBatch(eventList);
                            }
                        }
                    }
                    //将剩余数据推送
                    synchronized (eventList) {
                        if (!eventList.isEmpty()) {
                            flushEventBatch(eventList);
                        }
                    }
                } catch (Exception e) {
                    logger.error("Failed while running command: " + command, e);
                    if (e instanceof InterruptedException) {
                        //打断之后外部thread会重新启动
                        Thread.currentThread().interrupt();
                    }
                } finally {
                    if (reader != null) {
                        try {
                            reader.close();
                        } catch (IOException ex) {
                            logger.error("Failed to close reader for exec source", ex);
                        }
                    }
                    exitCode = String.valueOf(kill());
                }
                //等一段时间重启
                if (restart) {
                    logger.info("Restarting in {}ms, exit code {}", restartThrottle,
                            exitCode);
                    try {
                        Thread.sleep(restartThrottle);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                } else {
                    logger.info("Command [" + command + "] exited with " + exitCode);
                }
            } while (restart);
        }

        private void flushEventBatch(List<Event> eventList) {
            channelProcessor.processEventBatch(eventList);
            sourceCounter.addToEventAcceptedCount(eventList.size());
            eventList.clear();
            lastPushToChannel = systemClock.currentTimeMillis();
        }

        private boolean timeout() {
            return (systemClock.currentTimeMillis() - lastPushToChannel) >= batchTimeout;
        }

        private static String[] formulateShellCommand(String shell, String command) {
            String[] shellArgs = shell.split("\\s+");
            String[] result = new String[shellArgs.length + 1];
            System.arraycopy(shellArgs, 0, result, 0, shellArgs.length);
            result[shellArgs.length] = command;
            return result;
        }

        public int kill() {
            if (process != null) {
                synchronized (process) {
                    process.destroy();

                    try {
                        int exitValue = process.waitFor();

                        // Stop the Thread that flushes periodically
                        if (future != null) {
                            future.cancel(true);
                        }

                        if (timedFlushService != null) {
                            timedFlushService.shutdown();
                            while (!timedFlushService.isTerminated()) {
                                try {
                                    timedFlushService.awaitTermination(500, TimeUnit.MILLISECONDS);
                                } catch (InterruptedException e) {
                                    logger.debug("Interrupted while waiting for exec executor service "
                                            + "to stop. Just exiting.");
                                    Thread.currentThread().interrupt();
                                }
                            }
                        }
                        return exitValue;
                    } catch (InterruptedException ex) {
                        Thread.currentThread().interrupt();
                    }
                }
                return Integer.MIN_VALUE;
            }
            return Integer.MIN_VALUE / 2;
        }
        public void setRestart(boolean restart) {
            this.restart = restart;
        }
    }

    private static class StderrReader extends Thread {
        private BufferedReader input;
        private boolean logStderr;

        protected StderrReader(BufferedReader input, boolean logStderr) {
            this.input = input;
            this.logStderr = logStderr;
        }

        @Override
        public void run() {
            try {
                int i = 0;
                String line = null;
                while ((line = input.readLine()) != null) {
                    if (logStderr) {
                        // There is no need to read 'line' with a charset
                        // as we do not to propagate it.
                        // It is in UTF-16 and would be printed in UTF-8 format.
                        logger.info("StderrLogger[{}] = '{}'", ++i, line);
                    }
                }
            } catch (IOException e) {
                logger.info("StderrLogger exiting", e);
            } finally {
                try {
                    if (input != null) {
                        input.close();
                    }
                } catch (IOException ex) {
                    logger.error("Failed to close stderr reader for exec source", ex);
                }
            }
        }
    }
}
