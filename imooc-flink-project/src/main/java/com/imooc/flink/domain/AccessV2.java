package com.imooc.flink.domain;

public class AccessV2 {

    public String id;
    public String device;
    public String deviceType;
    public String os;
    public String event;
    public String net;
    public String channel;
    public String uid;
    public int nu;
    public String ip;
    public long time;
    public String version;
    public String province;
    public String city;
    public String day;
    public String hour;

    @Override
    public String toString() {
        return "AccessV2{" +
                "id='" + id + '\'' +
                ", device=" + device + '\'' +
                ", deviceType=" + deviceType + '\'' +
                ", os='" + os + '\'' +
                ", event='" + event + '\'' +
                ", net='" + net + '\'' +
                ", uid='" + uid + '\'' +
                ", nu=" + nu +
                ", ip='" + ip + '\'' +
                ", time=" + time +
                ", version='" + version + '\'' +
                ", province='" + province + '\'' +
                ", city='" + city + '\'' +
                ", day='" + day + '\'' +
                ", hour='" + hour + '\'' +
                '}';
    }
}
