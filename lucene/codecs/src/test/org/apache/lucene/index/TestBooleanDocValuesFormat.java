/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.lucene.index;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.function.LongSupplier;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.bool.BooleanDocValuesFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.TestUtil;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

/**
 * Tests BooleanDocValuesFormat. Inspired by {@link BaseDocValuesFormatTestCase}.
 */
public class TestBooleanDocValuesFormat extends BaseIndexFileFormatTestCase {
    private final Codec codec = TestUtil.alwaysDocValuesFormat(new BooleanDocValuesFormat());

    @Override
    protected Codec getCodec() {
        return codec;
    }

    @Override
    protected void addRandomFields(Document doc) {
        if (usually()) {
            doc.add(new NumericDocValuesField("ndv", random().nextInt(2)));
        }
    }

    public void testOneBoolean() throws IOException {
        Directory directory = newDirectory();
        RandomIndexWriter iwriter = new RandomIndexWriter(random(), directory);
        Document doc = new Document();
        String longTerm = "longtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongterm";
        String text = "This is the text to be indexed. " + longTerm;
        doc.add(newTextField("fieldname", text, Field.Store.YES));
        doc.add(new NumericDocValuesField("dv", 1));
        iwriter.addDocument(doc);
        iwriter.close();

        // Now search the index:
        IndexReader ireader = DirectoryReader.open(directory); // read-only=true
        IndexSearcher isearcher = new IndexSearcher(ireader);

        assertEquals(1, isearcher.count(new TermQuery(new Term("fieldname", longTerm))));
        Query query = new TermQuery(new Term("fieldname", "text"));
        TopDocs hits = isearcher.search(query, 1);
        assertEquals(1, hits.totalHits.value);
        // Iterate through the results:
        for (int i = 0; i < hits.scoreDocs.length; i++) {
            Document hitDoc = isearcher.doc(hits.scoreDocs[i].doc);
            assertEquals(text, hitDoc.get("fieldname"));
            assert ireader.leaves().size() == 1;
            NumericDocValues dv = ireader.leaves().get(0).reader().getNumericDocValues("dv");
            int docID = hits.scoreDocs[i].doc;
            assertEquals(docID, dv.advance(docID));
            assertEquals(1, dv.longValue());
        }

        ireader.close();
        directory.close();
    }

    public void testTwoBooleans() throws IOException {
        Directory directory = newDirectory();
        RandomIndexWriter iwriter = new RandomIndexWriter(random(), directory);
        Document doc = new Document();
        String longTerm = "longtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongterm";
        String text = "This is the text to be indexed. " + longTerm;
        doc.add(newTextField("fieldname", text, Field.Store.YES));
        doc.add(new NumericDocValuesField("dv1", 1));
        doc.add(new NumericDocValuesField("dv2", 0));
        iwriter.addDocument(doc);
        iwriter.close();

        // Now search the index:
        IndexReader ireader = DirectoryReader.open(directory); // read-only=true
        IndexSearcher isearcher = new IndexSearcher(ireader);

        assertEquals(1, isearcher.count(new TermQuery(new Term("fieldname", longTerm))));
        Query query = new TermQuery(new Term("fieldname", "text"));
        TopDocs hits = isearcher.search(query, 1);
        assertEquals(1, hits.totalHits.value);
        // Iterate through the results:
        for (int i = 0; i < hits.scoreDocs.length; i++) {
            int docID = hits.scoreDocs[i].doc;
            Document hitDoc = isearcher.doc(docID);
            assertEquals(text, hitDoc.get("fieldname"));
            assert ireader.leaves().size() == 1;
            NumericDocValues dv = ireader.leaves().get(0).reader().getNumericDocValues("dv1");
            assertEquals(docID, dv.advance(docID));
            assertEquals(1, dv.longValue());
            dv = ireader.leaves().get(0).reader().getNumericDocValues("dv2");
            assertEquals(NO_MORE_DOCS, dv.advance(docID));
        }

        ireader.close();
        directory.close();
    }

    public void testTwoDocumentsBoolean() throws IOException {
        Analyzer analyzer = new MockAnalyzer(random());

        Directory directory = newDirectory();
        IndexWriterConfig conf = newIndexWriterConfig(analyzer);
        conf.setMergePolicy(newLogMergePolicy());
        RandomIndexWriter iwriter = new RandomIndexWriter(random(), directory, conf);
        Document doc = new Document();
        doc.add(new NumericDocValuesField("dv", 1));
        iwriter.addDocument(doc);
        doc = new Document();
        doc.add(new NumericDocValuesField("dv", 0));
        iwriter.addDocument(doc);
        iwriter.forceMerge(1);
        iwriter.close();

        // Now search the index:
        IndexReader ireader = DirectoryReader.open(directory); // read-only=true
        assert ireader.leaves().size() == 1;
        NumericDocValues dv = ireader.leaves().get(0).reader().getNumericDocValues("dv");
        assertEquals(0, dv.nextDoc());
        assertEquals(1, dv.longValue());
        assertEquals(NO_MORE_DOCS, dv.nextDoc());

        ireader.close();
        directory.close();
    }

    public void testTwoDocumentsMerged() throws IOException {
        Analyzer analyzer = new MockAnalyzer(random());

        Directory directory = newDirectory();
        IndexWriterConfig conf = newIndexWriterConfig(analyzer);
        conf.setMergePolicy(newLogMergePolicy());
        RandomIndexWriter iwriter = new RandomIndexWriter(random(), directory, conf);
        Document doc = new Document();
        doc.add(newField("id", "0", StringField.TYPE_STORED));
        doc.add(new NumericDocValuesField("dv", 0));
        iwriter.addDocument(doc);
        iwriter.commit();
        doc = new Document();
        doc.add(newField("id", "1", StringField.TYPE_STORED));
        doc.add(new NumericDocValuesField("dv", 1));
        iwriter.addDocument(doc);
        iwriter.forceMerge(1);
        iwriter.close();

        // Now search the index:
        IndexReader ireader = DirectoryReader.open(directory); // read-only=true
        assert ireader.leaves().size() == 1;
        NumericDocValues dv = ireader.leaves().get(0).reader().getNumericDocValues("dv");
        assertEquals(1, dv.nextDoc());
        assertEquals(1, dv.longValue());
        assertEquals(NO_MORE_DOCS, dv.nextDoc());

        ireader.close();
        directory.close();
    }

    public void doTestBooleansVsStoredFields(LongSupplier longs) throws Exception {
        Directory dir = newDirectory();
        IndexWriterConfig conf = newIndexWriterConfig(new MockAnalyzer(random()));
        RandomIndexWriter writer = new RandomIndexWriter(random(), dir, conf);
        Document doc = new Document();
        Field idField = new StringField("id", "", Field.Store.NO);
        Field storedField = newStringField("stored", "", Field.Store.YES);
        Field dvField = new NumericDocValuesField("dv", 0);
        doc.add(idField);
        doc.add(storedField);
        doc.add(dvField);

        // index some docs
        int numDocs = atLeast(300);
        // numDocs should be always > 256 so that in case of a codec that optimizes
        // for numbers of values <= 256, all storage layouts are tested
        assert numDocs > 256;
        for (int i = 0; i < numDocs; i++) {
            idField.setStringValue(Integer.toString(i));
            long value = longs.getAsLong();
            storedField.setStringValue(Long.toString(value));
            dvField.setLongValue(value);
            writer.addDocument(doc);
            if (random().nextInt(31) == 0) {
                writer.commit();
            }
        }

        // delete some docs
        int numDeletions = random().nextInt(numDocs / 10);
        for (int i = 0; i < numDeletions; i++) {
            int id = random().nextInt(numDocs);
            writer.deleteDocuments(new Term("id", Integer.toString(id)));
        }

        // merge some segments and ensure that at least one of them has more than
        // 256 values
        writer.forceMerge(numDocs / 256);

        writer.close();

        // compare
        DirectoryReader ir = DirectoryReader.open(dir);
        TestUtil.checkReader(ir);
        for (LeafReaderContext context : ir.leaves()) {
            LeafReader r = context.reader();
            NumericDocValues docValues = DocValues.getNumeric(r, "dv");
            docValues.nextDoc();
            for (int i = 0; i < r.maxDoc(); i++) {
                String storedValue = r.document(i).get("stored");
                if (storedValue == null) {
                    assertTrue(docValues.docID() > i);
                } else {
                    long value = Long.parseLong(storedValue);
                    if (value == 0) {
                        continue;
                    }
                    int docID = docValues.docID();
                    assertEquals(i, docID);
                    assertEquals(Long.parseLong(storedValue), docValues.longValue());
                    docValues.nextDoc();
                }
            }
            assertEquals(NO_MORE_DOCS, docValues.docID());
        }
        ir.close();
        dir.close();
    }

    public void testBooleansVsStoredFields() throws Exception {
        int numIterations = atLeast(1);
        for (int i = 0; i < numIterations; i++) {
            doTestBooleansVsStoredFields(() -> random().nextInt(2));
        }
    }

    public void testTwoBooleansOneMissing() throws IOException {
        Directory directory = newDirectory();
        IndexWriterConfig conf = newIndexWriterConfig(null);
        conf.setMergePolicy(newLogMergePolicy());
        RandomIndexWriter iw = new RandomIndexWriter(random(), directory, conf);
        Document doc = new Document();
        doc.add(new StringField("id", "0", Field.Store.YES));
        doc.add(new NumericDocValuesField("dv1", 0));
        iw.addDocument(doc);
        doc = new Document();
        doc.add(new StringField("id", "1", Field.Store.YES));
        iw.addDocument(doc);
        iw.forceMerge(1);
        iw.close();

        IndexReader ir = DirectoryReader.open(directory);
        assertEquals(1, ir.leaves().size());
        LeafReader ar = ir.leaves().get(0).reader();
        NumericDocValues dv = ar.getNumericDocValues("dv1");
        assertEquals(NO_MORE_DOCS, dv.nextDoc());
        ir.close();
        directory.close();
    }

    public void testTwoBooleansOneMissingWithMerging() throws IOException {
        Directory directory = newDirectory();
        IndexWriterConfig conf = newIndexWriterConfig(null);
        conf.setMergePolicy(newLogMergePolicy());
        RandomIndexWriter iw = new RandomIndexWriter(random(), directory, conf);
        Document doc = new Document();
        doc.add(new StringField("id", "0", Field.Store.YES));
        doc.add(new NumericDocValuesField("dv1", 0));
        iw.addDocument(doc);
        iw.commit();
        doc = new Document();
        doc.add(new StringField("id", "1", Field.Store.YES));
        iw.addDocument(doc);
        iw.forceMerge(1);
        iw.close();

        IndexReader ir = DirectoryReader.open(directory);
        assertEquals(1, ir.leaves().size());
        LeafReader ar = ir.leaves().get(0).reader();
        NumericDocValues dv = ar.getNumericDocValues("dv1");
        assertEquals(NO_MORE_DOCS, dv.nextDoc());
        ir.close();
        directory.close();
    }

    public void testThreeBooleansOneMissingWithMerging() throws IOException {
        Directory directory = newDirectory();
        IndexWriterConfig conf = newIndexWriterConfig(null);
        conf.setMergePolicy(newLogMergePolicy());
        RandomIndexWriter iw = new RandomIndexWriter(random(), directory, conf);
        Document doc = new Document();
        doc.add(new StringField("id", "0", Field.Store.YES));
        doc.add(new NumericDocValuesField("dv1", 0));
        iw.addDocument(doc);
        doc = new Document();
        doc.add(new StringField("id", "1", Field.Store.YES));
        iw.addDocument(doc);
        iw.commit();
        doc = new Document();
        doc.add(new StringField("id", "2", Field.Store.YES));
        doc.add(new NumericDocValuesField("dv1", 1));
        iw.addDocument(doc);
        iw.forceMerge(1);
        iw.close();

        IndexReader ir = DirectoryReader.open(directory);
        assertEquals(1, ir.leaves().size());
        LeafReader ar = ir.leaves().get(0).reader();
        NumericDocValues dv = ar.getNumericDocValues("dv1");
        assertEquals(2, dv.nextDoc());
        assertEquals(1, dv.longValue());
        ir.close();
        directory.close();
    }

    /**
     * Tests dv against stored fields with threads (binary/numeric/sorted, no missing)
     */
    public void testThreads() throws Exception {
        Directory dir = newDirectory();
        IndexWriterConfig conf = newIndexWriterConfig(new MockAnalyzer(random()));
        RandomIndexWriter writer = new RandomIndexWriter(random(), dir, conf);
        Document doc = new Document();
        Field idField = new StringField("id", "", Field.Store.NO);
        Field storedNumericField = new StoredField("storedNum", "");
        Field dvNumericField = new NumericDocValuesField("dvNum", 0);
        doc.add(idField);
        doc.add(storedNumericField);
        doc.add(dvNumericField);

        // index some docs
        int numDocs = atLeast(300);
        for (int i = 0; i < numDocs; i++) {
            idField.setStringValue(Integer.toString(i));
            Integer numericValue = random().nextInt(2);
            storedNumericField.setStringValue(numericValue.toString());
            dvNumericField.setLongValue(numericValue);
            writer.addDocument(doc);
            if (random().nextInt(31) == 0) {
                writer.commit();
            }
        }

        // delete some docs
        int numDeletions = random().nextInt(numDocs / 10);
        for (int i = 0; i < numDeletions; i++) {
            int id = random().nextInt(numDocs);
            writer.deleteDocuments(new Term("id", Integer.toString(id)));
        }
        writer.close();

        // compare
        final DirectoryReader ir = DirectoryReader.open(dir);
        int numThreads = TestUtil.nextInt(random(), 2, 7);
        Thread threads[] = new Thread[numThreads];
        final CountDownLatch startingGun = new CountDownLatch(1);

        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread() {
                @Override
                public void run() {
                    try {
                        startingGun.await();
                        for (LeafReaderContext context : ir.leaves()) {
                            LeafReader r = context.reader();
                            NumericDocValues numerics = r.getNumericDocValues("dvNum");
                            for (int j = 0; j < r.maxDoc(); j++) {
                                String expected = r.document(j).get("storedNum");
                                long value = Long.parseLong(expected);
                                if (value == 0) {
                                    continue;
                                }
                                assertEquals(j, numerics.nextDoc());
                                assertEquals(1, numerics.longValue());
                            }
                        }
                        TestUtil.checkReader(ir);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            };
            threads[i].start();
        }
        startingGun.countDown();
        for (Thread t : threads) {
            t.join();
        }
        ir.close();
        dir.close();
    }

    /**
     * Tests dv against stored fields with threads (all types + missing)
     */
    @Slow
    public void testThreads2() throws Exception {
        Directory dir = newDirectory();
        IndexWriterConfig conf = newIndexWriterConfig(new MockAnalyzer(random()));
        RandomIndexWriter writer = new RandomIndexWriter(random(), dir, conf);
        Field idField = new StringField("id", "", Field.Store.NO);
        Field storedNumericField = new StoredField("storedNum", "");
        Field dvNumericField = new NumericDocValuesField("dvNum", 0);

        // index some docs
        int numDocs = TestUtil.nextInt(random(), 1025, 2047);
        for (int i = 0; i < numDocs; i++) {
            idField.setStringValue(Integer.toString(i));
            int numericValue = random().nextInt(2);
            storedNumericField.setStringValue(Integer.toString(numericValue));
            dvNumericField.setLongValue(numericValue);
            Document doc = new Document();
            doc.add(idField);
            if (random().nextInt(4) > 0) {
                doc.add(storedNumericField);
                doc.add(dvNumericField);
            }
            writer.addDocument(doc);
            if (random().nextInt(31) == 0) {
                writer.commit();
            }
        }

        // delete some docs
        int numDeletions = random().nextInt(numDocs / 10);
        for (int i = 0; i < numDeletions; i++) {
            int id = random().nextInt(numDocs);
            writer.deleteDocuments(new Term("id", Integer.toString(id)));
        }
        writer.close();

        // compare
        final DirectoryReader ir = DirectoryReader.open(dir);
        int numThreads = TestUtil.nextInt(random(), 2, 7);
        Thread threads[] = new Thread[numThreads];
        final CountDownLatch startingGun = new CountDownLatch(1);

        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread() {
                @Override
                public void run() {
                    try {
                        startingGun.await();
                        for (LeafReaderContext context : ir.leaves()) {
                            LeafReader r = context.reader();
                            NumericDocValues numerics = r.getNumericDocValues("dvNum");
                            for (int j = 0; j < r.maxDoc(); j++) {
                                String number = r.document(j).get("storedNum");
                                if (number != null) {
                                    long value = Long.parseLong(number);
                                    if (value == 0) {
                                        continue;
                                    }
                                    if (numerics != null) {
                                        assertEquals(j, numerics.advance(j));
                                        assertEquals(1, numerics.longValue());
                                    }
                                }
                            }
                        }
                        TestUtil.checkReader(ir);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            };
            threads[i].start();
        }
        startingGun.countDown();
        for (Thread t : threads) {
            t.join();
        }
        ir.close();
        dir.close();
    }

    public void testBooleanMergeAwayAllValues() throws IOException {
        Directory directory = newDirectory();
        Analyzer analyzer = new MockAnalyzer(random());
        IndexWriterConfig iwconfig = newIndexWriterConfig(analyzer);
        iwconfig.setMergePolicy(newLogMergePolicy());
        RandomIndexWriter iwriter = new RandomIndexWriter(random(), directory, iwconfig);

        Document doc = new Document();
        doc.add(new StringField("id", "0", Field.Store.NO));
        iwriter.addDocument(doc);
        doc = new Document();
        doc.add(new StringField("id", "1", Field.Store.NO));
        doc.add(new NumericDocValuesField("field", 1));
        iwriter.addDocument(doc);
        iwriter.commit();
        iwriter.deleteDocuments(new Term("id", "1"));
        iwriter.forceMerge(1);

        DirectoryReader ireader = iwriter.getReader();
        iwriter.close();

        NumericDocValues dv = getOnlyLeafReader(ireader).getNumericDocValues("field");
        assertEquals(NO_MORE_DOCS, dv.nextDoc());

        ireader.close();
        directory.close();
    }

    // same as testBooleanMergeAwayAllValues but on more than 1024 docs to have sparse encoding on
    public void testNumericMergeAwayAllValuesLargeSegment() throws IOException {
        Directory directory = newDirectory();
        Analyzer analyzer = new MockAnalyzer(random());
        IndexWriterConfig iwconfig = newIndexWriterConfig(analyzer);
        iwconfig.setMergePolicy(newLogMergePolicy());
        RandomIndexWriter iwriter = new RandomIndexWriter(random(), directory, iwconfig);

        Document doc = new Document();
        doc.add(new StringField("id", "1", Field.Store.NO));
        doc.add(new NumericDocValuesField("field", 1));
        iwriter.addDocument(doc);
        final int numEmptyDocs = atLeast(1024);
        for (int i = 0; i < numEmptyDocs; ++i) {
            iwriter.addDocument(new Document());
        }
        iwriter.commit();
        iwriter.deleteDocuments(new Term("id", "1"));
        iwriter.forceMerge(1);

        DirectoryReader ireader = iwriter.getReader();
        iwriter.close();

        NumericDocValues dv = getOnlyLeafReader(ireader).getNumericDocValues("field");
        assertEquals(NO_MORE_DOCS, dv.nextDoc());

        ireader.close();
        directory.close();
    }
}
