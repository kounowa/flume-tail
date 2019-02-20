import sun.font.TrueTypeFont;

public class NewTailSourceConfigurationConstants {
    /**
     * Should the exec'ed command restarted if it dies: : default false
     */
    public static final String CONFIG_RESTART = "restart";

    public static final boolean DEFAULT_RESTART = true;

    /**
     * Amount of time to wait before attempting a restart: : default 10000 ms
     */
    public static final String CONFIG_RESTART_THROTTLE = "restartThrottle";
    public static final long DEFAULT_RESTART_THROTTLE = 10000L;

    /**
     * Should stderr from the command be logged: default false
     */
    public static final String CONFIG_LOG_STDERR = "logStdErr";
    public static final boolean DEFAULT_LOG_STDERR = false;

    /**
     * Number of lines to read at a time
     */
    public static final String CONFIG_BATCH_SIZE = "batchSize";
    public static final int DEFAULT_BATCH_SIZE = 20;

    /**
     * Amount of time to wait, if the buffer size was not reached, before
     * to data is pushed downstream: : default 3000 ms
     */
    public static final String CONFIG_BATCH_TIME_OUT = "batchTimeout";
    public static final long DEFAULT_BATCH_TIME_OUT = 3000L;

    /**
     * Charset for reading input
     */
    public static final String CHARSET = "charset";
    public static final String DEFAULT_CHARSET = "UTF-8";

    /**
     * Optional shell/command processor used to run command
     */
    public static final String CONFIG_SHELL = "shell";

    /**
     * dir which to spoll
     */
    public static final String SPOLL_DIR = "spolldir";

    /**
     * 枚举类用于配置文件按照啥时间更换文件
     *
     * per_day代表类似logger.2019-02-19.log的文件
     * 可自行添加
     *
     */
    public enum Time_Type{
        per_day
    }

    /**
     * 新增文件是否名字默认，如log4j每次生成一个logger.log 而昨天的文件会命名为昨天的日期
     * 不相同的话则为类似文件格式为logger.2018-10-19.log
     */
    public static final String NEW_FILE_IF_SAME = "new_file_if_same";
    public static final boolean DEFAULT_NEW_FILE_IF_SAME = true;

    /**
     * file format
     * 最终 文件会根据DEFAULT_NEW_FILE_IF_SAME和time_type生成类似logger.2018-10-19.log的文件
     * 配置文件配置FILE_FORMAT的时候可以配置类似logger.{per_day}.log
     */
    public static final String FILE_FORMAT = "file_format";
    public static final String DEFAULT_FILE_FORMAT = "logger.{time_type}.log";


}
