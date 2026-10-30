package com.ai.socket;

import lombok.extern.slf4j.Slf4j;
import java.io.*;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

@Slf4j
public class SocketClientHandler implements Runnable {

    private final Socket socket;

    public SocketClientHandler(Socket socket) {
        this.socket = socket;
    }

    @Override
    public void run() {
        handle();
    }

    private void handle() {
        // 获取客户端地址用于日志
        String remoteAddr = socket.getRemoteSocketAddress().toString();

        System.out.println("处理客户端连接: " + remoteAddr);
        // try-with-resources 自动关闭流和 Socket
        try (
             InputStream in = socket.getInputStream();
             PrintWriter writer = new PrintWriter(
                     new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8), true)) {

            // 设置读取超时，防止连接假死
            socket.setSoTimeout(30_000);

            System.out.println("客户端已连接: " + remoteAddr);
            log.info("客户端已连接: {}", remoteAddr);

            byte[] buffer = new byte[4096];
            int len;

            while ((len = in.read(buffer)) != -1) {
                String msg = new String(buffer, 0, len, StandardCharsets.UTF_8);
                log.info("收到 [{}] 消息: {}", remoteAddr, msg);

                String response = "echo: " + msg;
                writer.println(response);
            }

            log.info("客户端正常断开: {}", remoteAddr);

        } catch (IOException e) {
            // 区分是正常断开还是异常
            if (e instanceof java.net.SocketTimeoutException) {
                log.warn("客户端 [{}] 读取超时", remoteAddr);
            } else {
                log.warn("客户端 [{}] 连接异常", remoteAddr, e);
            }
        } finally {
            // 确保资源释放（虽然 try-with-resources 已经做了，但显式关闭更稳妥）
            closeQuietly(socket);
        }
    }

    private void closeQuietly(Socket socket) {
        if (socket != null && !socket.isClosed()) {
            try {
                socket.close();
            } catch (IOException e) {
                log.error("关闭 Socket 失败", e);
            }
        }
    }
}