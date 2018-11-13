package lsp.lucene.index;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Date;

import lsp.lucene.ik.IKAnalyzer6x;

public class CreateIndex {
    public static void main(String[] args) throws IOException {
        createIndex();
//        updateDocument();
//        deleteDocument();
    }

    private static void createIndex() throws IOException {
        News news1 = new News();
        news1.setId(1);
        news1.setTitle("习近平会见美国总统奥巴马，学习国外经验");
        news1.setContent("国家主席习近平9月2日在杭州西湖国宾馆会见前来出席二十国集团领导人杭州峰会的美国总统奥巴马...");
        news1.setReply(672);

        News news2 = new News();
        news2.setId(2);
        news2.setTitle("北大迎4380名新生 农村学生700多人近年最多");
        news2.setContent("昨天，北京大学迎来4380名来自全国各地及数十个国家的本科新生。其中，农村学生共700余名，为近年最多...");
        news2.setReply(995);

        News news3 = new News();
        news3.setId(3);
        news3.setTitle("特朗普宣誓就任美国第45任总统");
        news3.setContent("当地时间1月20日，唐纳德.特朗普在美国国会宣誓就职，正式成为美国第45任总统...");
        news3.setReply(1872);

        Date start = new Date();
        Analyzer analyzer = new IKAnalyzer6x();
        IndexWriterConfig iwc = new IndexWriterConfig(analyzer);
        iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
        Path indexPath = Paths.get("indexdir");
        Directory dir = FSDirectory.open(indexPath);
        IndexWriter indexWriter = new IndexWriter(dir, iwc);

        FieldType idType = new FieldType();
        idType.setIndexOptions(IndexOptions.DOCS);
        idType.setStored(true);

        FieldType titleType = new FieldType();
        titleType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
        titleType.setStored(true);
        titleType.setTokenized(true);

        FieldType contentType = new FieldType();
        contentType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
        contentType.setStored(true);
        contentType.setTokenized(true);
        contentType.setStoreTermVectors(true);
        contentType.setStoreTermVectorPositions(true);
        contentType.setStoreTermVectorOffsets(true);
        contentType.setStoreTermVectorPayloads(true);

        Document doc1 = new Document();
        doc1.add(new Field("id", String.valueOf(news1.getId()), idType));
        doc1.add(new Field("title", news1.getTitle(), titleType));
        doc1.add(new Field("content", news1.getContent(), contentType));
        doc1.add(new IntPoint("reply", news1.getReply()));
        doc1.add(new StoredField("reply_display", news1.getReply()));

        Document doc2 = new Document();
        doc2.add(new Field("id", String.valueOf(news2.getId()), idType));
        doc2.add(new Field("title", news2.getTitle(), titleType));
        doc2.add(new Field("content", news2.getContent(), contentType));
        doc2.add(new IntPoint("reply", news2.getReply()));
        doc2.add(new StoredField("reply_display", news2.getReply()));

        Document doc3 = new Document();
        doc3.add(new Field("id", String.valueOf(news3.getId()), idType));
        doc3.add(new Field("title", news3.getTitle(), titleType));
        doc3.add(new Field("content", news3.getContent(), contentType));
        doc3.add(new IntPoint("reply", news3.getReply()));
        doc3.add(new StoredField("reply_display", news3.getReply()));

        indexWriter.addDocument(doc1);
        indexWriter.addDocument(doc2);
        indexWriter.addDocument(doc3);

        indexWriter.commit();
        indexWriter.close();
        dir.close();
        Date end = new Date();
        System.out.println("索引文档用时：" + (end.getTime() - start.getTime()) + "毫秒");
    }

    private static void updateDocument() throws IOException {
        Analyzer analyzer = new IKAnalyzer6x();
        IndexWriterConfig iwc = new IndexWriterConfig(analyzer);
        Path indexPath = Paths.get("indexdir");
        Directory dir = FSDirectory.open(indexPath);
        IndexWriter indexWriter = new IndexWriter(dir, iwc);
        Document document = new Document();
        document.add(new TextField("id", "2", Field.Store.YES));
        document.add(new TextField("title", "北京大学迎4380名新生 农村学生700多人近年最多", Field.Store.YES));
        document.add(new TextField("content", "昨天，北京大学迎来4380名来自全国各地及数十个国家的本科新生。其中，农村学生共700余名，为近年最多...", Field.Store.YES));
        indexWriter.updateDocument(new Term("title", "北大"), document);
        indexWriter.commit();
        indexWriter.close();
        dir.close();
        System.out.println("更新完成");
    }

    private static void deleteDocument() throws IOException {
        Analyzer analyzer = new IKAnalyzer6x();
        IndexWriterConfig iwc = new IndexWriterConfig(analyzer);
        Path indexPath = Paths.get("indexdir");
        Directory dir = FSDirectory.open(indexPath);
        IndexWriter indexWriter = new IndexWriter(dir, iwc);
        indexWriter.deleteDocuments(new Term("title", "北京大学"));
        indexWriter.commit();
        indexWriter.close();
        dir.close();
        System.out.println("删除完成");
    }
}
