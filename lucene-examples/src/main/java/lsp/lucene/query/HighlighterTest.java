package lsp.lucene.query;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.highlight.Fragmenter;
import org.apache.lucene.search.highlight.Highlighter;
import org.apache.lucene.search.highlight.InvalidTokenOffsetsException;
import org.apache.lucene.search.highlight.QueryScorer;
import org.apache.lucene.search.highlight.SimpleHTMLFormatter;
import org.apache.lucene.search.highlight.SimpleSpanFragmenter;
import org.apache.lucene.search.highlight.TokenSources;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import lsp.lucene.ik.IKAnalyzer6x;

public class HighlighterTest {

    public static void main(String[] args) throws ParseException, IOException, InvalidTokenOffsetsException {
        Path indexPath = Paths.get("indexdir");
        Directory dir = FSDirectory.open(indexPath);
        IndexReader reader = DirectoryReader.open(dir);
        IndexSearcher searcher = new IndexSearcher(reader);
        String field = "title";
        Analyzer analyzer = new IKAnalyzer6x(true);
        QueryParser parser = new QueryParser(field, analyzer);
        parser.setDefaultOperator(QueryParser.Operator.AND);
        Query query = parser.parse("农村学生");
        System.out.println("Query:" + query.toString());

        // 关键字高亮
        QueryScorer score = new QueryScorer(query, field);
        SimpleHTMLFormatter fors = new SimpleHTMLFormatter("<span style='color:red;'>", "</span>");
        Highlighter highlighter = new Highlighter(fors, score);

        TopDocs tds = searcher.search(query, 10);
        for (ScoreDoc sd : tds.scoreDocs) {
            Document doc = searcher.doc(sd.doc);
            System.out.println("DocId:" + sd.doc);
            System.out.println("Id:" + doc.get("id"));
            System.out.println("title:" + doc.get("title"));
            System.out.println("文档平分:" + sd.score);
            TokenStream tokenStream = TokenSources.getAnyTokenStream(searcher.getIndexReader(), sd.doc, field, analyzer);
            Fragmenter fragmenter = new SimpleSpanFragmenter(score);
            highlighter.setTextFragmenter(fragmenter);
            String str = highlighter.getBestFragment(tokenStream, doc.get(field));
            System.out.println("高亮片段:" + str);
        }
        dir.close();
        reader.close();
    }
}
