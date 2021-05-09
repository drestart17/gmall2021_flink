package cn.srt.bigdata.gmall.realtime.utils;

import cn.srt.bigdata.gmall.realtime.bean.TableProcess;
import com.google.common.base.CaseFormat;
import org.apache.commons.beanutils.BeanUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;


public class MySQLUtil {


    //查询数据表
    public static<T> List<T> queryList(String sql,Class<T> clazz,boolean underScoreToCamel) {

        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;

        try {
            //1.注册驱动
            Class.forName("com.mysql.jdbc.Driver");
            //2.创建连接
            conn = DriverManager.getConnection("jdbc:mysql://bigdata-test03:3306/mall_realtime_test?characterEncoding=utf-8&useSSL=false", "root", "big#Data1&good");
            //3.创建数据库操作对象
            ps = conn.prepareStatement(sql);
            //4.执行sql语句,得到一个ResultSet结果集
            rs = ps.executeQuery();
            //5.处理结果集，得到结果集的元数据
            ResultSetMetaData rsMetaData = rs.getMetaData();
            //6.创建List，封装返回结果
            List<T> resultList = new ArrayList<>();
            //7.根据ResultSet，每循环一次获取一条查询结果
            while(rs.next()) {

                //7.0定义每一行数据是什么类型
                T obj = clazz.newInstance();
                //7.1循环每一列，取出所有列的数据
                for (int i = 1; i <= rsMetaData.getColumnCount(); i++) {
                    //7.1.1获取每列数据的列名
                    String columnName = rsMetaData.getColumnName(i);
                    //如果开启了驼峰命名法，则需要转换
                    if(underScoreToCamel){
                        columnName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, columnName);
                    }
                    //7.1.2给Bean的属性赋值(i第几列，rs.getObject(i):返回的是第i列的value)
                    BeanUtils.setProperty(obj,columnName,rs.getObject(i));
                }
                //7.2 将bean对象的每一条记录放到list中
                resultList.add(obj);
            }
            return resultList;
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("查询mysql失败！");
        //8.关闭连接
        }finally {
            if(rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if(ps != null) {
                try {
                    ps.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if(conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    //测试
//    public static void main(String[] args) {
//        String sql = "select * from table_process";
//        List<TableProcess> tableProcessList = queryList(sql, TableProcess.class, true);
//        for (TableProcess tableProcess : tableProcessList) {
//            System.out.println(tableProcess);
//        }
//    }
}
