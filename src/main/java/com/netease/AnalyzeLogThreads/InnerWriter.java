package com.netease.AnalyzeLogThreads;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;

/**
 * Description: 大文件写入操作
 * <p>
 *     从队列中获取一组数据进行处理, 处理之后写入到文件
 *     文件名根据输出文件名 + 进程id命名
 * </p>
 * Create:      2018/7/2 23:46
 *
 * @author Yang Meng(eyangmeng@163.com)
 */
public class InnerWriter implements Runnable {

    public InnerWriter() {}

    public InnerWriter(BlockingQueue<List<String>> blockingQueue, String outputFileName) {
        this.blockingQueue = blockingQueue;
        this.outputFileName = outputFileName;
    }

    @Override
    public void run() {
        List<String> dataGroup = blockingQueue.poll();
        if (dataGroup == null) {
            return;
        }
        // 读取到结束标识， 重新入队
        if (dataGroup.isEmpty()) {
            blockingQueue.add(END);
            return;
        }
        String output = String.format("%s.%s", outputFileName, Thread.currentThread().getId());
        File outputFile = new File(output);
        try {
            FileWriter writer = new FileWriter(outputFile);
            for (String line : dataGroup) {
                writer.write(String.format("%s\t%s\n", line, line.length()));
            }
            writer.close();
        } catch (Exception e) {
            LOG.error(e.getMessage());
        }
        dataGroup.clear();
    }

    /** 阻塞队列 */
    private BlockingQueue<List<String>> blockingQueue;

    /** 输出文件名 */
    private String outputFileName;

    /** 结束标识 */
    private static final List<String> END = new ArrayList<>();

    private static final Logger LOG = LoggerFactory.getLogger(InnerWriter.class);
}