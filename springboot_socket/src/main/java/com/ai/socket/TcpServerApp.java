package com.ai.socket;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TcpServerApp {

    private static final Logger log = LoggerFactory.getLogger(TcpServerApp.class);

    // 客户端请求的端口号
    private static final int CLIENT_PORT = 12001;
    // 服务端本地监听端口（配置里的“本地监听端口”）
    private static final int SERVER_PORT = 18081;

    // 1. 业务逻辑处理耗时（需小于客户端的30秒超时）
    private static final int MAX_BUSINESS_TIME_MS = 25000;
    // 2. 应用层心跳发送间隔（毫秒），防止防火墙/系统层 KeepAlive 清理
    private static final int HEARTBEAT_INTERVAL_MS = 10000;

    public static void main(String[] args) {
        // 创建服务端 Socket 并绑定本地监听端口
        try (ServerSocket serverSocket = new ServerSocket(SERVER_PORT)) {
            log.info("TCP 服务端已启动，本地监听端口: {}，等待客户端 [{}] 连接...", SERVER_PORT, CLIENT_PORT);

            // 使用线程池处理并发连接（避免多线程创建销毁开销）
            ExecutorService threadPool = Executors.newFixedThreadPool(20);

            while (true) {
                // 阻塞式等待客户端连接，超时由线程池控制
                Socket clientSocket = serverSocket.accept();

                // 异步处理每一个客户端长连接
                threadPool.submit(() -> handleConnection(clientSocket));
            }
        } catch (IOException e) {
            log.error("服务端启动或接受连接失败", e);
        }
    }

    private static void handleConnection(Socket socket) {
        // 必须设置超时，防止线程永久阻塞！
        try {
            socket.setSoTimeout(30000); // 30秒读数据超时，完美匹配客户端配置
            socket.setKeepAlive(true);  // 启用系统级保活
            socket.setTcpNoDelay(true); // 禁用 Nagle 算法，保证小包及时发送

            String remoteAddr = socket.getInetAddress() + ":" + socket.getPort();
            log.info("新连接已建立，来源: {}，握手方式: 客户端方", remoteAddr);

            // 获取输入输出流
            InputStream inputStream = socket.getInputStream();
            OutputStream outputStream = socket.getOutputStream();

            // 启动后台心跳线程（守护线程，不阻塞主业务）
            Thread heartBeatThread = new Thread(() -> {
                try {
                    byte[] heartBeatPkg = "HEARTBEAT".getBytes(); // 自定义心跳报文
                    while (!socket.isClosed()) {
                        Thread.sleep(HEARTBEAT_INTERVAL_MS);
                        // 发送心跳包
                        outputStream.write(heartBeatPkg);
                        outputStream.flush();
                        log.debug("向 [{}] 发送应用层心跳包...", remoteAddr);
                    }
                } catch (SocketException e) {
                    log.warn("连接 [{}] 异常断开，心跳发送失败: {}", remoteAddr, e.getMessage());
                } catch (Exception e) {
                    log.error("心跳线程异常", e);
                }
            });
            heartBeatThread.setDaemon(true);
            heartBeatThread.start();

            // 循环处理客户端发来的同步请求
            byte[] buffer = new byte[1024];
            while (true) {
                try {
                    int bytesRead;
                    try {
                        // 30秒内没有数据，抛出 SocketTimeoutException
                        bytesRead = inputStream.read(buffer);
                    } catch (SocketTimeoutException e) {
                        log.warn("读取 [{}] 数据超时，继续等待...", remoteAddr);
                        continue;
                    }

                    // 客户端正常关闭连接
                    if (bytesRead == -1) {
                        log.info("客户端 [{}] 已正常关闭连接", remoteAddr);
                        break;
                    }

                    // 接收到的业务数据
                    byte[] receivedData = new byte[bytesRead];
                    System.arraycopy(buffer, 0, receivedData, 0, bytesRead);
                    log.info("收到 [{}] 数据，长度: {}", remoteAddr, receivedData.length);

                    // 过滤心跳包
                    if (new String(receivedData).contains("HEARTBEAT")) {
                        log.debug("收到 [{}] 心跳响应包，连接正常", remoteAddr);
                        continue;
                    }

                    // TODO: 在这里执行你的业务逻辑（如查库、报文解析等）
                    // 必须控制业务处理时间 < 25秒，否则客户端30秒超时后会发 RST 包！
                    String businessResult = processBusiness(receivedData);

                    // 返回业务应答给客户端（单向认证：只验证对方即可）
                    outputStream.write(businessResult.getBytes());
                    outputStream.flush();
                    log.info("向 [{}] 返回业务应答", remoteAddr);

                } catch (java.net.SocketException e) {
                    // 典型触发场景：客户端超时断开、进程重启、网络波动
                    log.warn("连接 [{}] 发生 Connection reset (客户端主动断开): {}", remoteAddr, e.getMessage());
                    break;
                } catch (Exception e) {
                    log.error("处理 [{}] 业务时发生严重异常", remoteAddr, e);
                    break;
                }
            }
        } catch (IOException e) {
            log.error("处理连接时发生底层异常", e);
        } finally {
            // 最终资源清理，防止僵尸连接和文件描述符泄漏
            try {
                if (socket != null && !socket.isClosed()) {
                    socket.close();
                }
                log.info("连接 [{}:{}] 已彻底关闭，资源已释放",
                        socket.getInetAddress(), socket.getPort());
            } catch (IOException e) {
                log.error("关闭连接失败", e);
            }
        }
    }

    private static String processBusiness(byte[] requestData) throws InterruptedException {
        // 模拟耗时业务处理
        Thread.sleep(500);
        return "ACK_OK"; // 返回约定的应答报文
    }
}