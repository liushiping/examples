package lsp.lucene.query;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import lsp.lucene.ik.IKAnalyzer6x;

public class QueryParseTest {

    public static void main(String[] args) throws IOException, ParseException {
        Path indexPath = Paths.get("indexdir");
        Directory dir = FSDirectory.open(indexPath);
        IndexReader reader = DirectoryReader.open(dir);
        IndexSearcher searcher = new IndexSearcher(reader);
        Query query = booleanQuery();
        System.out.println("Query:" + query.toString());
        TopDocs tds = searcher.search(query, 10);
        for (ScoreDoc sd : tds.scoreDocs) {
            Document doc = searcher.doc(sd.doc);
            System.out.println("DocId:" + sd.doc);
            System.out.println("Id:" + doc.get("id"));
            System.out.println("title:" + doc.get("title"));
            System.out.println("文档平分:" + sd.score);
        }
        dir.close();
        reader.close();
    }

    private static Query queryParser() throws ParseException {
        String field = "title";
        Analyzer analyzer = new IKAnalyzer6x(true);
        QueryParser parser = new QueryParser(field, analyzer);
        parser.setDefaultOperator(QueryParser.Operator.AND);
        return parser.parse("农村学生");
    }

    private static Query multiFieldQuertParser() throws ParseException {
        String[] field = {"title", "content"};
        Analyzer analyzer = new IKAnalyzer6x(true);
        QueryParser parser = new MultiFieldQueryParser(field, analyzer);
        return parser.parse("总统");
    }

    private static Query termQuery() {
        Term term = new Term("title", "学生");
        return new TermQuery(term);
    }

    private static Query booleanQuery() {
        Query query1 = new TermQuery(new Term("title", "总统"));
        Query query2 = new TermQuery(new Term("content", "杭州"));
        BooleanClause bc1 = new BooleanClause(query1, BooleanClause.Occur.MUST);
        BooleanClause bc2 = new BooleanClause(query2, BooleanClause.Occur.MUST_NOT);
        return new BooleanQuery.Builder().add(bc1).add(bc2).build();
    }
}
