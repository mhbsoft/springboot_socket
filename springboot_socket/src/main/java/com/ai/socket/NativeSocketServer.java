package com.ai.socket;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

@Component
@Slf4j
public class NativeSocketServer {

    private int port = 12001;

    private ServerSocket serverSocket;
    private ExecutorService executor;
    private volatile boolean running = true;

    @PostConstruct
    public void start() {

        ThreadFactory threadFactory = new ThreadFactory() {
            private final AtomicInteger counter = new AtomicInteger(1);
            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r, "cesp-thread-" + counter.getAndIncrement());
                t.setDaemon(false);
                return t;
            }
        };

        executor = new ThreadPoolExecutor(
                Runtime.getRuntime().availableProcessors() * 2,  // 核心线程数
                Runtime.getRuntime().availableProcessors() * 2,  // 最大线程数
                0L, TimeUnit.MILLISECONDS,                        // 空闲线程存活时间
                new LinkedBlockingQueue<>(1024),                  // 有界工作队列
                threadFactory, // 自定义线程名
                new ThreadPoolExecutor.CallerRunsPolicy()         // 拒绝策略：调用者线程执行
        );

        Thread acceptThread = new Thread(this::acceptLoop, "socket-accept");
        acceptThread.setDaemon(true);
        acceptThread.start();
    }

    private void acceptLoop() {
        try {
            serverSocket = new ServerSocket(port);
            log.info("原生 Socket 服务已启动，监听端口：{}", port);

            while (running && !Thread.currentThread().isInterrupted()) {
                try {
                    Socket clientSocket = serverSocket.accept();
                    log.info("收到客户端连接: {}", clientSocket.getRemoteSocketAddress());
                    executor.execute(new SocketClientHandler(clientSocket));
                } catch (IOException e) {
                    if (running) {
                        log.error("接受客户端连接失败", e);
                    }
                }
            }
        } catch (IOException e) {
            log.error("Socket 服务启动失败", e);
        }
    }

    @PreDestroy
    public void stop() {
        running = false;

        try {
            if (serverSocket != null && !serverSocket.isClosed()) {
                serverSocket.close();
            }
        } catch (IOException e) {
            log.error("关闭 ServerSocket 失败", e);
        }

        if (executor != null) {
            executor.shutdownNow();
        }

        log.info("原生 Socket 服务已关闭");
    }
}