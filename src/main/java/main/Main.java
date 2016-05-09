package main;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.*;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.search.similarities.SimilarityBase;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.commons.io.FileUtils;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.util.Properties;
import java.util.TreeMap;

public class Main {
    //path to the TREC library, take a small amount for testing purposes
    private static String docsPath;
    private static String topicsPath;
    private static String indexPath;
    private static String setupIndex;
    private static String similarity;
    final static int hitsPerPage = 1000;
    final static boolean debugOutput = true;

    public static void main(String[] args) throws IOException, ParseException {
        loadProperties();

        StandardAnalyzer analyzer = new StandardAnalyzer();
        Similarity bm25 = getSimilarity();

        //1. creates the index, sets up to use BM25Similarity
        // and Standard Analyzer and indexes TREC files
        if(setupIndex.equals("true")) {
            setUpIndex(analyzer, bm25);
        }
        //2. parse the list of topics to be queried
        TreeMap<String, String> topics = setUpTopicMap(topicsPath);
        // 3. search for the topics in the index
        //delete the results File for new run
        new File("resultsFile" + similarity).delete();
        searchForTopicsInIndex(topics, analyzer, bm25);
    }

    public static Similarity getSimilarity() {
        if(similarity.equals("VA")) {
            return new BM25VASimilarity();
        } else if(similarity.equals("ORIGINAL")) {
            System.out.println("Use BM 25 Original Similarity");
            return new BM25SimilarityOriginal();
        } else {
            System.out.println("Use Lucene default Similarity");
            return null;
        }
    }


    /**
     * This method executes all the queries we specified on the index
     * @param topics the topics we want to use as search terms
     * @param analyzer the preprocessor used
     * @param bm25 the similarity measure used
     * @throws ParseException
     * @throws IOException
     */
    static void searchForTopicsInIndex(TreeMap<String, String> topics, Analyzer analyzer, Similarity bm25) throws ParseException, IOException {
        //path to index is opened
        Directory index = FSDirectory.open(new File(indexPath).toPath());
        IndexReader reader = DirectoryReader.open(index);
        IndexSearcher searcher = new IndexSearcher(reader);
        if(bm25 != null) {
            searcher.setSimilarity(bm25);
        }
        //create ordered list out of keys
        for(String key : topics.keySet()) {
            // create specific query
            String querystr = topics.get(key);

            // the "title" arg specifies the default field to use
            // when no field is explicitly specified in the query.
            Query q = new QueryParser("contents", analyzer).parse(querystr);
            //execute query
            TopDocs docs = searcher.search(q, hitsPerPage);
            //number of hits
            ScoreDoc[] hits = docs.scoreDocs;

            // display results
            if(debugOutput) {
                System.out.println("Found " + hits.length + " hits for topic no. " + key + " - " + topics.get(key));
            }
            for(int i=0;i<hits.length;++i) {
                int docId = hits[i].doc;
                float score = hits[i].score;
                Document d = searcher.doc(docId);
                String result = key + " Q0" + " " + d.get("docno") + " " + (i + 1) +  " " + score + " grp2-" + similarity;
                if(debugOutput) {
                    System.out.println(result);
                }
                File file = new File("resultsFile" + similarity);
                FileUtils.writeStringToFile(file, result + "\n", Charset.defaultCharset(), true);

            }
        }

        // reader can only be closed when there
        // is no need to access the documents any more.
        reader.close();
    }

    /**
     * This method reads the topics file into a TreeMap (like a hashMap but sorted Keys)
     * @param topicsPath the path for the topicsTREC8Adhoc.txt
     * @return a map with all the topics, the topic number as key and topic title as value
     * @throws IOException
     */
    static TreeMap<String, String> setUpTopicMap(String topicsPath) throws IOException {
        TreeMap<String, String> topics = new TreeMap<>();
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
        if(bm25 != null) {
            config.setSimilarity(bm25);
        }
        //our index we write entries to, is on file system
        FSDirectory index = FSDirectory.open(new File(indexPath).toPath());
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
                    if (doc != null && doc.getField("contents") != null) {
                        writer.addDocument(doc);
                    }
                }
            }
        }
    }

    static void loadProperties() {
        File configFile = new File("config.properties");
        try {
            FileReader reader = new FileReader(configFile);
            Properties props = new Properties();
            props.load(reader);
            docsPath = props.getProperty("docs");
            topicsPath = props.getProperty("topics");
            setupIndex = props.getProperty("setupIndex");
            similarity = props.getProperty("similarity");
            indexPath = props.getProperty("index");
            if(similarity.equals("VA")) {
                indexPath += "VA/";
            } else if(similarity.equals("ORIGINAL")){
                indexPath += "ORIGINAL/";
            } else {
                indexPath += "default/";
            }

        } catch (FileNotFoundException ex) {
            // file does not exist
        } catch (IOException ex) {
            // I/O error
        }
    }
}