package main;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.lucene.document.*;
import org.apache.lucene.index.IndexOptions;

/**
 * Taken from https://github.com/isoboroff/trec-demo/blob/master/src/TrecDocIterator.java
 */
public class TrecDocIterator implements Iterator<Document> {

    protected BufferedReader rdr;
    protected boolean at_eof = false;

    public TrecDocIterator(File file) throws FileNotFoundException {
        rdr = new BufferedReader(new FileReader(file));
        if(Main.debugOutput)
            System.out.println("Reading " + file.toString());
    }

    @Override
    public boolean hasNext() {
        return !at_eof;
    }

    @Override
    public Document next() {
        Document doc = new Document();
        StringBuffer sb = new StringBuffer();
        try {
            String line;
            Pattern docno_tag = Pattern.compile("<DOCNO>\\s*(\\S+)\\s*<");
            boolean in_doc = false;
            while (true) {
                line = rdr.readLine();
                if (line == null) {
                    at_eof = true;
                    break;
                }
                if (!in_doc) {
                    if (line.startsWith("<DOC>"))
                        in_doc = true;
                    else
                        continue;
                }
                if (line.startsWith("</DOC>")) {
                    in_doc = false;
                    sb.append(line);
                    break;
                }

                Matcher m = docno_tag.matcher(line);
                if (m.find()) {
                    String docno = m.group(1);
                    doc.add(new StringField("docno", docno, Field.Store.YES));
                }

                sb.append(line);
            }
            if (sb.length() > 0){
                FieldType type = new FieldType();
                type.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
                type.setTokenized(true);
                type.setStored(false);
                type.setStoreTermVectors(true);
                type.setStoreTermVectorPositions(true);
                type.freeze();
                doc.add(new Field("contents", sb.toString(), type));
            }

        } catch (IOException e) {
            doc = null;
        }
        return doc;
    }

    @Override
    public void remove() {
        // Do nothing, but don't complain
    }

}