package com.imooc.flink.domain;

public class OrderInfo {
    public String orderId;
    public long time;
    public double money;

    @Override
    public String toString() {
        return "OrderInfo{" +
                "orderId='" + orderId + '\'' +
                ", time=" + time +
                ", money=" + money +
                '}';
    }
}
