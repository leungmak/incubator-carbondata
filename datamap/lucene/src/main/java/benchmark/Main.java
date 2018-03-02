package benchmark;

import java.io.IOException;
import java.nio.file.Paths;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.FSDirectory;

public class Main {
  public static final String INDEX_DIRECTORY = "/Users/jacky/code/carbondata/datamap/lucene/indexDirectory";

  public static void main(String[] args) throws Exception {

    createIndex();
    searchIndex("IDENTIFIER9");
    searchIndex("IDENTIFIER*");
    searchIndex("*111");
    searchIndex("IDENTIFIERxyz*");
    searchIndex("IDENTIFIER1*1");

  }

  private static void createIndex() throws IOException {
    Analyzer analyzer = new StandardAnalyzer();
    IndexWriter indexWriter = new IndexWriter(
        FSDirectory.open(Paths.get(INDEX_DIRECTORY)),
        new IndexWriterConfig(analyzer));

    for (int id = 0; id < 100; id++) {
      Document document = new Document();
      String value = "IDENTIFIER" + id;
      System.out.println("adding '" + value + "'");
      document.add(new TextField("id", value, Field.Store.YES));
      indexWriter.addDocument(document);
    }
    indexWriter.commit();
    indexWriter.close();
  }

  static IndexReader indexReader;

  static {
    try {
      indexReader = DirectoryReader.open(FSDirectory.open(Paths.get(INDEX_DIRECTORY)));
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  static IndexSearcher indexSearcher = new IndexSearcher(indexReader);

  private static void searchIndex(String searchString) throws IOException {
    System.out.println("Searching for '" + searchString + "'");

    Analyzer analyzer = new StandardAnalyzer();

    QueryParser queryParser = new QueryParser("id", analyzer);
    queryParser.setAllowLeadingWildcard(true);
    Query query;
    try {
      query = queryParser.parse(searchString);
    } catch (org.apache.lucene.queryparser.classic.ParseException e) {
      throw new IOException(e);
    }

    // execute index search
    TopDocs result;
    try {
      result = indexSearcher.search(query, 10);
    } catch (IOException e) {
      String errorMessage =
          String.format("failed to search lucene data, detail is %s", e.getMessage());
      throw new IOException(errorMessage);
    }

    System.out.println(result.scoreDocs.length + " hits");
    for (ScoreDoc scoreDoc : result.scoreDocs) {
      Document document = indexSearcher.doc(scoreDoc.doc);
      String id = document.get("id");
      System.out.println("Hit: " + id);
    }

  }
}
