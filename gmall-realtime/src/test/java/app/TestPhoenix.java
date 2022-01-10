package app;


import com.szl.gmall.realtime.common.GmallConfig;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class TestPhoenix {

    public static void main(String[] args) throws Exception {

        Class.forName(GmallConfig.PHOENIX_DRIVER);
        Connection connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);

        PreparedStatement preparedStatement = connection.prepareStatement("select * from GMALL210625_REALTIME.DIM_BASE_TRADEMARK");

        ResultSet resultSet = preparedStatement.executeQuery();

        while (resultSet.next()) {
            System.out.println("ID:" + resultSet.getObject(1) + ",TM_NAME:" + resultSet.getObject(2));
        }

        preparedStatement.close();
        connection.close();

    }

}
