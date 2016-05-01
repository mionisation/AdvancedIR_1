import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;

import java.io.File;
import java.io.IOException;

public class Main {
    //path to the TREC library, take a small amount for testing purposes
    final static String docsPath = "/home/mionisation/Dropbox/UNI SS 2016/InformationRetrievalVU/Exercise/Exercise0data/Adhoc/fr94/01";

    public static void main(String[] args) throws IOException, ParseException {
        //Use Standard Analyzer for pre processing the text
        StandardAnalyzer analyzer = new StandardAnalyzer();
        //create the index writer and set to use BM25Similarity
        BM25Similarity bm25 = new BM25Similarity();
        IndexWriterConfig config = new IndexWriterConfig(analyzer);
        config.setSimilarity(bm25);
        //our index we write entries to
        Directory index = new RAMDirectory();
        //init writer
        IndexWriter w = new IndexWriter(index, config);

        //index the docs in the docsPath
        File docDir = new File(docsPath);
        indexDocs(w, docDir);
        w.close();

        // 2. query
        String querystr = args.length > 0 ? args[0] : "copyright Valencia Oranges Grown in Arizona";

        // the "title" arg specifies the default field to use
        // when no field is explicitly specified in the query.
        Query q = new QueryParser("contents", analyzer).parse(querystr);

        // 3. search
        int hitsPerPage = 200;
        IndexReader reader = DirectoryReader.open(index);
        IndexSearcher searcher = new IndexSearcher(reader);
        searcher.setSimilarity(bm25);
        TopDocs docs = searcher.search(q, hitsPerPage);
        ScoreDoc[] hits = docs.scoreDocs;

        // 4. display results
        System.out.println("Found " + hits.length + " hits.");
        for(int i=0;i<hits.length;++i) {
            int docId = hits[i].doc;
            float score = hits[i].score;
            Document d = searcher.doc(docId);
            System.out.println("Q0" + " " + d.get("docno") + " " + (i + 1) +  " " + score );
        }

        // reader can only be closed when there
        // is no need to access the documents any more.
        reader.close();
    }

    static void indexDocs(IndexWriter writer, File file)
            throws IOException {
        // do not try to index files that cannot be read
        if (file.canRead()) {
            if (file.isDirectory()) {
                String[] files = file.list();
                // an IO error could occur
                if (files != null) {
                    for (int i = 0; i < files.length; i++) {
                        indexDocs(writer, new File(file, files[i]));
                    }
                }
            } else {
                TrecDocIterator docs = new TrecDocIterator(file);
                Document doc;
                while (docs.hasNext()) {
                    doc = docs.next();
                    if (doc != null && doc.getField("contents") != null)
                        writer.addDocument(doc);
                }
            }
        }
    }
}