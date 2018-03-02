package benchmark;

import java.io.IOException;
import java.nio.file.Paths;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.FSDirectory;

public class LuceneWriteIndexExample
{
  private static final String INDEX_DIR = "/Users/jacky/code/carbondata/datamap/lucene/lucene6index";

  public static void main(String[] args) throws Exception
  {
    IndexWriter writer = createWriter();

    //Let's clean everything first
    writer.deleteAll();

    for (int i = 0; i < 10 * 1000 * 1000; i++) {
      Document document1 = createDocument(i, "id" + i, "Gupta", "howtodoinjava.com");
      writer.addDocument(document1);
    }

    writer.commit();
    writer.close();
  }

  private static Document createDocument(Integer id, String firstName, String lastName, String website)
  {
    Document document = new Document();
    document.add(new StringField("id", id.toString() , Field.Store.YES));
    document.add(new TextField("firstName", firstName , Field.Store.YES));
    document.add(new TextField("lastName", lastName , Field.Store.YES));
    document.add(new TextField("website", website , Field.Store.YES));
    return document;
  }

  private static IndexWriter createWriter() throws IOException
  {
    FSDirectory dir = FSDirectory.open(Paths.get(INDEX_DIR));
    IndexWriterConfig config = new IndexWriterConfig(new StandardAnalyzer());
    IndexWriter writer = new IndexWriter(dir, config);
    return writer;
  }
}