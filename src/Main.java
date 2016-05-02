import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
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
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;

import java.io.*;
import java.util.HashMap;

public class Main {
    //path to the TREC library, take a small amount for testing purposes
    final static String docsPath = "/home/mionisation/Dropbox/UNI SS 2016/InformationRetrievalVU/Exercise/Exercise0data/Adhoc/fr94/01";
    final static String topicsPath = "/home/mionisation/Dropbox/UNI SS 2016/InformationRetrievalVU/Exercise/Exercise0data/topicsTREC8Adhoc.txt";

    public static void main(String[] args) throws IOException, ParseException {
        StandardAnalyzer analyzer = new StandardAnalyzer();
        BM25Similarity bm25 = new BM25Similarity();
        //creates the index, sets up to use BM25Similarity
        // and Standard Analyzer and indexes TREC files
        Directory index = setUpIndex(analyzer, bm25);

        HashMap<String, String> topics = setUpTopicMap(topicsPath);
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

    /**
     * This method reads the topics file into a hashmap
     * @param topicsPath the path for the topicsTREC8Adhoc.txt
     * @return a map with all the topics, the topic number as key and topic title as value
     * @throws IOException
     */
    static HashMap<String, String> setUpTopicMap(String topicsPath) throws IOException {
        HashMap<String, String> topics = new HashMap<>();
        BufferedReader br = new BufferedReader(new FileReader(topicsPath));
        String number = "default", title = "default";
        while(true) {
            String line = br.readLine();
            if(line == null)
                break;
            if(line.startsWith("<num> Number: ")) {
                number = line.substring(13);
            }
            if(line.startsWith("<title> ")) {
                title = line.substring(7);
                topics.put(number, title);
            }
        }
        return topics;
    }

    /**
     * This method sets up the index and feeds it with content to be indexed
     * @param analyzer the analyzer to be used to preprocess the data
     * @param bm25 the similarity function to be used
     * @return the collection of indexed entries
     * @throws IOException shouldn't happen :)
     */
    static Directory setUpIndex(Analyzer analyzer, Similarity bm25) throws IOException {
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
        return index;
    }

    /**
     * Iterates through the TREC library folders and indexes everything
     * @param writer allows us to write to the index
     * @param file any directory or file in the TREC folders
     * @throws IOException
     */
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