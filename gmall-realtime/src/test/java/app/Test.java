package app;

import com.szl.gmall.realtime.utils.DateTimeUtil;

import java.util.Date;

public class Test {

    public static void main(String[] args) {

        //2022-01-07 12:12:12
        System.out.println(DateTimeUtil.toYMDhms(new Date(1641557532000L)));

        System.out.println(DateTimeUtil.toTs("2022-01-08 10:50:21"));


    }

}
