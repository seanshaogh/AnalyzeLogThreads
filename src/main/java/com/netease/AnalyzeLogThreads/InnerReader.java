package com.netease.AnalyzeLogThreads;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Description: 读取文件进程实现
 * <p>
 *     逐行读取文件中的数据，按照指定数量组成一组，放入阻塞队列
 *     读取结束，放入结束标识
 * </p>
 * Create:      2018/7/2 23:30
 *
 * @author Yang Meng(eyangmeng@163.com)
 */
public class InnerReader implements Runnable {

    public InnerReader() {}

    public InnerReader(BlockingQueue<List<String>> blockingQueue, String inputFileName, int maxQueueSize) {
        this.blockingQueue = blockingQueue;
        this.inputFileName = inputFileName;
        this.maxQueueSize = maxQueueSize;
    }

    @Override
    public void run() {
        File inputFile = new File(inputFileName);
        if (!inputFile.exists()) {
            LOG.error("cat not find such file: {}", inputFileName);
            return;
        }
        try {
            BufferedReader bufferedReader = new BufferedReader(new FileReader(inputFile));
            String line;
            List<String> dataGroup = new ArrayList<>(GROUP_LIMIT);
            while ((line = bufferedReader.readLine()) != null) {
                if (dataGroup.size() >= GROUP_LIMIT) {
                    addDataGroup(dataGroup);
                    // 数据重置
                    dataGroup = new ArrayList<>(GROUP_LIMIT);
                }
                dataGroup.add(line);
            }
            // 最后一批数据
            addDataGroup(dataGroup);

            // 结束标识
            addDataGroup(END);
            bufferedReader.close();
        } catch (Exception e) {
            LOG.error(e.getMessage());
        }
    }

    /**
     * 阻塞队列中加入数据: 队列满则等待
     *
     * @param dataGroup 一组数据
     * @throws Exception time.sleep
     */
    private void addDataGroup(List<String> dataGroup) throws Exception{
        while (blockingQueue.size() >= maxQueueSize) {
            TimeUnit.SECONDS.sleep(3);
        }
        blockingQueue.add(dataGroup);
    }


    /** 阻塞队列 */
    private BlockingQueue<List<String>> blockingQueue;

    /** 输入文件名称 */
    private String inputFileName;

    /** 队列最大长度 */
    private int maxQueueSize;

    /** 一组数据大小 */
    private static final int GROUP_LIMIT = 1024;

    /** 结束标识 */
    private static final List<String> END = new ArrayList<>();

    private static final Logger LOG = LoggerFactory.getLogger(InnerReader.class);
}
