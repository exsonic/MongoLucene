package MongoLucene;

import com.mongodb.*;
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
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

import java.io.File;


public class App {
	public static final Version luceneVersion = Version.LUCENE_46;

	public static void main(String[] args) throws Exception {
//		args = new String[]{"index", "player", "keyword"};
//		args = new String[]{"Kobe"};
		if (args[0].equals("index")) {
			index(args[1], args[2]);
		} else {
			search(args[0]);
		}
    }

	static void index(String tableName, String fieldName) throws Exception {
		MongoClient mongoClient = new MongoClient();
		DB db = mongoClient.getDB("Lytic");
		DBCollection table = db.getCollection(tableName);
		DBCursor cursor = table.find();

		Directory index = FSDirectory.open(new File("lucene.index"));

		StandardAnalyzer analyzer = new StandardAnalyzer(luceneVersion);
		IndexWriterConfig config = new IndexWriterConfig(luceneVersion, analyzer);
		IndexWriter indexWriter = new IndexWriter(index, config);

		while (cursor.hasNext()) {
			DBObject object = cursor.next();
			Document doc = new Document();
			doc.add(new StringField("id", object.get("_id").toString(), Field.Store.YES));
			doc.add(new TextField("text", object.get(fieldName).toString(), Field.Store.YES));
			indexWriter.addDocument(doc);
		}
		cursor.close();
		indexWriter.close();
	}

	static void search(String searchString) throws  Exception{
		Directory index = FSDirectory.open(new File("lucene.index"));

		// the "title" arg specifies the default field to use
		// when no field is explicitly specified in the query.
		Query q = new QueryParser(luceneVersion, "text", new StandardAnalyzer(luceneVersion)).parse(searchString);

		// 3. search
		int hitsPerPage = 10;
		IndexReader reader = DirectoryReader.open(index);
		IndexSearcher searcher = new IndexSearcher(reader);
		TopScoreDocCollector collector = TopScoreDocCollector.create(hitsPerPage, true);
		searcher.search(q, collector);
		ScoreDoc[] hits = collector.topDocs().scoreDocs;

		// 4. display results
		System.out.println("Found " + hits.length + " hits.");
		for (int i = 0; i < hits.length; ++i) {
			Document d = searcher.doc(hits[i].doc);
			System.out.println((i + 1) + ". " + d.get("id") + "\t" + d.get("text"));
		}

		// reader can only be closed when there
		// is no need to access the documents any more.
		reader.close();
	}
}