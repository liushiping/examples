package lsp.lucene.analyzer;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.cjk.CJKAnalyzer;
import org.apache.lucene.analysis.cn.smart.SmartChineseAnalyzer;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.analysis.core.SimpleAnalyzer;
import org.apache.lucene.analysis.core.StopAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

import java.io.IOException;
import java.io.StringReader;

import lsp.lucene.ik.IKAnalyzer6x;

public class VariousAnalyzers {
    private static String str = "中华人民共和国简称中国，是一个有13亿人口的国家";

    private static void printAnalyzer(Analyzer analyzer) throws IOException {
        StringReader reader = new StringReader(str);
        TokenStream tokenStream = analyzer.tokenStream(str, reader);
        tokenStream.reset();
        CharTermAttribute attribute = tokenStream.getAttribute(CharTermAttribute.class);
        System.out.println("分词结果：");
        while (tokenStream.incrementToken()) {
            System.out.print(attribute.toString() + "|");
        }
        System.out.println("\n");
        analyzer.close();
    }

    public static void main(String[] args) throws IOException {
        Analyzer analyzer = new StandardAnalyzer();
        System.out.println("标准分词：" + analyzer.getClass());
        printAnalyzer(analyzer);

        analyzer = new SimpleAnalyzer();
        System.out.println("简单分词：" + analyzer.getClass());
        printAnalyzer(analyzer);

        analyzer = new CJKAnalyzer();
        System.out.println("二分法分词：" + analyzer.getClass());
        printAnalyzer(analyzer);

        analyzer = new KeywordAnalyzer();
        System.out.println("关键字分词：" + analyzer.getClass());
        printAnalyzer(analyzer);

        analyzer = new StopAnalyzer();
        System.out.println("停用词分词：" + analyzer.getClass());
        printAnalyzer(analyzer);

        analyzer = new SmartChineseAnalyzer();
        System.out.println("中文智能分词：" + analyzer.getClass());
        printAnalyzer(analyzer);

        analyzer = new IKAnalyzer6x();
        System.out.println("IK默认分词：" + analyzer.getClass());
        printAnalyzer(analyzer);

        analyzer = new IKAnalyzer6x(true);
        System.out.println("IK智能分词：" + analyzer.getClass());
        printAnalyzer(analyzer);
    }
}
