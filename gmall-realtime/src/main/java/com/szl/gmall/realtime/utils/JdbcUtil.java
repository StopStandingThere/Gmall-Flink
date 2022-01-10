package com.szl.gmall.realtime.utils;

import com.google.common.base.CaseFormat;
import org.apache.commons.beanutils.BeanUtils;

import java.lang.reflect.InvocationTargetException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class JdbcUtil {

    public static <T> List<T> queryData(Connection connection, String sql, Class<T> clz, Boolean toCamel) throws SQLException, IllegalAccessException, InstantiationException, InvocationTargetException {

        //创建一个集合用来存放结果数据
        ArrayList<T> resultList = new ArrayList<>();

        //编译sql
        PreparedStatement preparedStatement = connection.prepareStatement(sql);

        //执行查询
        ResultSet resultSet = preparedStatement.executeQuery();

        //获取元数据信息
        ResultSetMetaData metaData = resultSet.getMetaData();

        //获取列数
        int columnCount = metaData.getColumnCount();

        //遍历resultSet,将每行数据转换为T对象
        while (resultSet.next()){

            //创建T对象
            T t = clz.newInstance();

            //遍历每一列 获取列名 获取每一行数据的值
            for (int i = 0; i < columnCount; i++) {
                String columnName = metaData.getColumnName(i + 1);
                Object value = resultSet.getObject(columnName);

                //转换为小驼峰命名形式
                if (toCamel){
                    columnName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL,columnName.toLowerCase());
                }

                //给T对象赋值
                BeanUtils.setProperty(t,columnName,value);
            }
            //将T对象存放至结果集合
            resultList.add(t);
        }
        //关闭资源
        resultSet.close();
        preparedStatement.close();

        return resultList;
    }
}
