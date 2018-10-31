package com.netease.AnalyzeLogThreads;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.*;

/**
 * Description: 大文件读写多线程操作操作
 * <p>
 *     1. 一个reader逐行读取文件中的数据，按照指定数量组成一组，放入阻塞队列
 *     2. 多个writer从阻塞队列中读取数据进行处理
 * </p>
 * Create:      2018/7/2 23:27
 *
 * @author Yang Meng(eyangmeng@163.com)
 */
public class BigFileApplication {

    public static void main(String[] args) {
        if (args == null || args.length < 2) {
            LOG.error("请提供输入文件和输出文件名称");
            return;
        }
        try {
            executor(args[0], args[1]);
        } catch (Exception e) {
            e.printStackTrace();
            LOG.error("执行器错误. {}", e.getMessage());
        }


    }

    private static void executor(String inputFile, String outputFile) throws Exception {
        BlockingQueue<List<String>> blockingQueue = new ArrayBlockingQueue<>(DEFAULT_QUEUE_SIZE);
        ThreadPoolExecutor pools = getThreadPool();
        pools.execute(new InnerReader(blockingQueue, inputFile, DEFAULT_QUEUE_SIZE));
        while (true) {
            if (blockingQueue.isEmpty()) {
                TimeUnit.SECONDS.sleep(1);
            }

            List<String> dataGroup = blockingQueue.peek();
            if (dataGroup != null && dataGroup.isEmpty()) {
                LOG.info("process the big file done.");
                break;
            }
            pools.execute(new InnerWriter(blockingQueue, outputFile));
        }
        pools.shutdown();
    }

    /**
     * 定义线程池
     *      TODO: 相关配置加入配置文件
     *
     * @return 线程池
     */
    private static ThreadPoolExecutor getThreadPool() {
        RejectedExecutionHandler handler = new ThreadPoolExecutor.CallerRunsPolicy();
        return new ThreadPoolExecutor(5, 10, 5, TimeUnit.SECONDS,new LinkedBlockingQueue<Runnable>(10),handler);
    }


    private static final int DEFAULT_QUEUE_SIZE = 30;
    private static final Logger LOG = LoggerFactory.getLogger(BigFileApplication.class);
}