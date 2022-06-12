package com.ab.gmall.realtime.util;

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

public class KeywordUtil {
    public static List<String> splitKeyWord(String keyword) throws IOException {
        //todo 创建集合存放结果数据
        ArrayList<String> result = new ArrayList<>();
        //todo 创建分词器对象
        StringReader reader = new StringReader(keyword);
        IKSegmenter ikSegmenter = new IKSegmenter(reader, true);
        //todo 提取分词
        Lexeme next = ikSegmenter.next();
        //todo 迭代取词
        while (next != null){
            String word = next.getLexemeText();

            result.add(word);

            next = ikSegmenter.next();

        }


        return result;
    }

    public static void main(String[] args) throws IOException {
        System.out.println(splitKeyWord("好吃的苹果派"));
    }
}
