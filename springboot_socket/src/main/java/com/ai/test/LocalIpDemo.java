package com.ai.test;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class LocalIpDemo {
    public static void main(String[] args) {
        try {
            InetAddress ip = InetAddress.getLocalHost();
            System.out.println("主机名：" + ip.getHostName());
            System.out.println("IP地址：" + ip.getHostAddress());
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }
}