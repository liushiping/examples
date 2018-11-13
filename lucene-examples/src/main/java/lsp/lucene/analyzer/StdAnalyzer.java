package lsp.lucene.analyzer;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

import java.io.IOException;
import java.io.StringReader;

public class StdAnalyzer {

    private static String strCh = "中华人民共和国简称中国，是一个有13亿人口的国家";
    private static String strEn = "Dogs can not archive a place, eyes can reach";

    private static void stdAnalyzer(String str) throws IOException {
        Analyzer analyzer = new StandardAnalyzer();
        StringReader reader = new StringReader(str);
        TokenStream tokenStream = analyzer.tokenStream(str, reader);
        tokenStream.reset();
        CharTermAttribute attribute = tokenStream.getAttribute(CharTermAttribute.class);
        System.out.println("分词结果：");
        while (tokenStream.incrementToken()) {
            System.out.print(attribute.toString() + "|");
        }
        System.out.println();
        analyzer.close();
     }

     public static void main(String[] args) throws IOException {
         System.out.println("中文分词结果：");
         stdAnalyzer(strCh);
         System.out.println("英文分词结果：");
         stdAnalyzer(strEn);
     }
}
