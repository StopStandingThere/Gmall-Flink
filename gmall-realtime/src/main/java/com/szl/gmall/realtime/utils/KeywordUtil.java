package com.szl.gmall.realtime.utils;

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

public class KeywordUtil {

    //使用IK分词器对字符串进行分词
    public static List<String> splitKeyword(String keyword) throws IOException {

        //创建一个集合用来存放结果数据
        ArrayList<String> keywordList = new ArrayList<>();

        //创建IK分词器对象
        StringReader reader = new StringReader(keyword);
        IKSegmenter ikSegmenter = new IKSegmenter(reader, false);

        Lexeme lexeme = ikSegmenter.next();

        while (lexeme!=null){
            String text = lexeme.getLexemeText();
            keywordList.add(text);
            lexeme = ikSegmenter.next();
        }

        return keywordList;
    }

    //测试分词器
    public static void main(String[] args) throws IOException {
        List<String> stringList = splitKeyword("你好世界,深深迷恋");
        for (String word : stringList) {
            System.out.println(word);
        }
    }
}
