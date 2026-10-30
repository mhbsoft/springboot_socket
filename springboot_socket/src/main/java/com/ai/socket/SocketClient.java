package com.ai.socket;

import java.io.*;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

public class SocketClient {

    private static final String HOST = "127.0.0.1";
    private static final int PORT = 12001;

    public static void main(String[] args) {
        try (Socket socket = new Socket(HOST, PORT);
             PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
             InputStream in = socket.getInputStream();
             BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {

            // 发送消息
            out.println("Hello Server");
            out.flush();
            System.out.println("已发送：" + "Hello Server");

            // 读取服务端响应
            String response = reader.readLine();
            System.out.println("收到响应：" + response);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}