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
package org.apache.lucene.search;

import java.io.IOException;
import java.util.Objects;

import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;

/**
 * Like {@code DocValuesNumbersQuery}, but this query only
 * runs on a long {@link NumericDocValuesField}, matching
 * all documents whose value is {@code 1}. All documents with
 * {@code 0} and {@code null} values are considered as absent.
 * <p>
 *
 * @lucene.experimental
 */
public class DocValuesBooleanQuery extends Query {

    private final String field;
  
    public DocValuesBooleanQuery(String field) {
        this.field = Objects.requireNonNull(field);
    }
  
    @Override
    public boolean equals(Object other) {
        return sameClassAs(other) && equalsTo(getClass().cast(other));
    }
  
    private boolean equalsTo(DocValuesBooleanQuery other) {
        return field.equals(other.field);
    }
  
    @Override
    public int hashCode() {
        return 31 * classHash() + Objects.hash(field);
    }
  
    @Override
    public void visit(QueryVisitor visitor) {
        if (visitor.acceptField(field)) {
            visitor.visitLeaf(this);
        }
    }
  
    @Override
    public String toString(String defaultField) {
      return "DocValuesBooleanQuery:" + field;
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) {
        return new ConstantScoreWeight(this, boost) {
            @Override
            public boolean isCacheable(LeafReaderContext ctx) {
                return true;
            }

            @Override
            public Scorer scorer(LeafReaderContext context) throws IOException {
                final NumericDocValues values = DocValues.getNumeric(context.reader(), field);
                final TwoPhaseIterator twoPhaseIterator = new TwoPhaseIterator(values) {
                    @Override
                    public boolean matches() throws IOException {
                        return values.longValue() != 0; // treats any non-zero value as true
                    }

                    @Override
                    public float matchCost() {
                        return 1; // 1 comparison
                    }
                };

                final DocIdSetIterator iterator = TwoPhaseIterator.asDocIdSetIterator(twoPhaseIterator);
                return new Scorer(this) {
                    @Override
                    public int docID() {
                        return iterator.docID();
                    }

                    @Override
                    public float score() {
                        return boost;
                    }

                    @Override
                    public DocIdSetIterator iterator() {
                        return iterator;
                    }

                    @Override
                    public float getMaxScore(int upTo) throws IOException {
                        return Float.POSITIVE_INFINITY;
                    }
                };
            }
        };
    }
}

