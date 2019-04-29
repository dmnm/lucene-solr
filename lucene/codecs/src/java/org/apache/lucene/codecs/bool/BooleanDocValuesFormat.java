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

package org.apache.lucene.codecs.bool;

import java.io.IOException;

import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;

/**
 * Bitwise doc values format that uses internal representation of {@link org.apache.lucene.util.FixedBitSet}
 * in order to store indexed values and load them at search time as a simple {code long[]} array without additional
 * decoding. There are several reasons for this:
 * <ul>
 * <li>At index time encoding is super fast without superfluous iterations over all values to choose ths best
 * compression algorithm suitable for given data.</li>
 * <li>At query time decoding is also simple and fast, no GC pressure and extra steps</li>
 * <li>Internal representation allows to perform random access in constant time</li>
 * </ul>
 *
 * <p>Limitations:
 * <ul>
 * <li>Does not support non boolean fields</li>
 * <li>Boolean fields must be represented as long values {@code 1} for {@code true} and {@code 0} for {@code false}</li>
 * </ul>
 * <p>
 * Current implementation does not support advanced bit set implementations like
 * {@link org.apache.lucene.util.SparseFixedBitSet} or {@code org.apache.lucene.util.RoaringDocIdSet}.
 */
public class BooleanDocValuesFormat extends DocValuesFormat {

    private static final String DATA_CODEC = "BooleanDocValuesData";
    private static final String DATA_EXTENSION = "dvdb";
    private static final String METADATA_CODEC = "BooleanDocValuesMetadata";
    private static final String METADATA_EXTENSION = "dvmb";

    public BooleanDocValuesFormat() {
        super("Boolean");
    }

    @Override
    public DocValuesConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
        return new BooleanDocValuesConsumer(state, DATA_CODEC, DATA_EXTENSION, METADATA_CODEC, METADATA_EXTENSION);
    }

    @Override
    public DocValuesProducer fieldsProducer(SegmentReadState state) throws IOException {
        return new BooleanDocValuesProducer(state, DATA_CODEC, DATA_EXTENSION, METADATA_CODEC, METADATA_EXTENSION);
    }

}
