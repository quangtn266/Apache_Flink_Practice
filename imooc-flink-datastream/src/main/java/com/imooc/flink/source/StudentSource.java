package com.imooc.flink.source;

import com.imooc.flink.utils.MySQLUtils;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import javax.security.auth.login.Configuration;
import java.sql.Connection;
import java.sql.PreparedStatement;

public class StudentSource extends RichSourceFunction<Student> {

    Connection connection;
    PreparedStatement psmt;

    @Override
    public void open(Configuration parameters) throws Exception {
        connection = MySQLUtils.getConnection();
        psmt = connection.prepareStatement("select * from student")
    }

    @Override
    public void close()
}
