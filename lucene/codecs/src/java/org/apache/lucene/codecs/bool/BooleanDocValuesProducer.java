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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.Accountables;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.RamUsageEstimator;

/**
 * Reader for {@link BooleanDocValuesFormat}
 */
class BooleanDocValuesProducer extends DocValuesProducer {
    // metadata maps (just file pointers and minimal stuff)
    private final Map<String, BooleanEntry> booleans = new HashMap<>();

    // ram instances we have already loaded
    private final Map<String, BitSet> booleanInstances = new HashMap<>();

    private final IndexInput data;
    private final int numEntries;

    private final int maxDoc;
    private long ramBytesUsed;
    private final int version;

    private final boolean merging;

    static final byte NUMBER = 0;

    private static final int VERSION_START = 0;
    static final int VERSION_CURRENT = VERSION_START;

    // clone for merge: when merging we don't do any instances.put()s
    private BooleanDocValuesProducer(BooleanDocValuesProducer original) {
        assert Thread.holdsLock(original);
        booleans.putAll(original.booleans);
        data = original.data.clone();

        booleanInstances.putAll(original.booleanInstances);

        numEntries = original.numEntries;
        maxDoc = original.maxDoc;
        ramBytesUsed = original.ramBytesUsed;
        version = original.version;
        merging = true;
    }

    BooleanDocValuesProducer(SegmentReadState state, String dataCodec, String dataExtension, String metaCodec,
                             String metaExtension) throws IOException {
        maxDoc = state.segmentInfo.maxDoc();
        merging = false;
        String metaName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, metaExtension);
        // read in the entries from the metadata file.
        ChecksumIndexInput in = state.directory.openChecksumInput(metaName, state.context);
        ramBytesUsed = RamUsageEstimator.shallowSizeOfInstance(getClass());
        boolean success = false;
        try {
            version = CodecUtil.checkIndexHeader(in, metaCodec, VERSION_START, VERSION_CURRENT,
                    state.segmentInfo.getId(), state.segmentSuffix);
            numEntries = readFields(in, state.fieldInfos);

            CodecUtil.checkFooter(in);
            success = true;
        } finally {
            if (success) {
                IOUtils.close(in);
            } else {
                IOUtils.closeWhileHandlingException(in);
            }
        }

        String dataName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, dataExtension);
        this.data = state.directory.openInput(dataName, state.context);
        success = false;
        try {
            final int version2 = CodecUtil.checkIndexHeader(data, dataCodec, VERSION_START, VERSION_CURRENT,
                    state.segmentInfo.getId(), state.segmentSuffix);
            if (version != version2) {
                throw new CorruptIndexException("Format versions mismatch: meta=" + version + ", data=" + version2, data);
            }

            // NOTE: data file is too costly to verify checksum against all the bytes on open,
            // but for now we at least verify proper structure of the checksum footer: which looks
            // for FOOTER_MAGIC + algorithmID. This is cheap and can detect some forms of corruption
            // such as file truncation.
            CodecUtil.retrieveChecksum(data);

            success = true;
        } finally {
            if (!success) {
                IOUtils.closeWhileHandlingException(this.data);
            }
        }
    }

    private BooleanEntry readBooleanEntry(IndexInput meta) throws IOException {
        BooleanEntry entry = new BooleanEntry();
        entry.offset = meta.readLong();
        entry.count = meta.readInt();
        return entry;
    }

    private int readFields(IndexInput meta, FieldInfos infos) throws IOException {
        int numEntries = 0;
        int fieldNumber = meta.readVInt();
        while (fieldNumber != -1) {
            numEntries++;
            FieldInfo info = infos.fieldInfo(fieldNumber);
            int fieldType = meta.readByte();
            if (fieldType == NUMBER) {
                booleans.put(info.name, readBooleanEntry(meta));
            } else {
                throw new CorruptIndexException("invalid entry type: " + fieldType + ", field= " + info.name, meta);
            }
            fieldNumber = meta.readVInt();
        }
        return numEntries;
    }

    @Override
    public long ramBytesUsed() {
        return ramBytesUsed;
    }

    @Override
    public synchronized Collection<Accountable> getChildResources() {
        Collection<Accountable> childResources = Accountables.namedAccountables("numeric field", booleanInstances);
        return Collections.unmodifiableList(new ArrayList<>(childResources));
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "(entries=" + numEntries + ")";
    }

    @Override
    public void checkIntegrity() throws IOException {
        CodecUtil.checksumEntireFile(data.clone());
    }

    @Override
    public synchronized NumericDocValues getNumeric(FieldInfo field) throws IOException {
        BitSet instance = booleanInstances.get(field.name);
        BooleanEntry ne = booleans.get(field.name);
        if (instance == null) {
            // Lazy load
            instance = loadBoolean(ne);
            if (!merging) {
                booleanInstances.put(field.name, instance);
                ramBytesUsed += instance.ramBytesUsed();
            }
        }
        return new BooleanDocValues(instance, ne.count << 6);
    }

    private BitSet loadBoolean(BooleanEntry entry) throws IOException {
        IndexInput data = this.data.clone();
        data.seek(entry.offset);

        int length = entry.count;
        final long[] bits = new long[length];
        for (int i = 0; i < length; i++) {
            //better to read bytes at a time org.apache.lucene.store.DataInput.readBytes(byte[], int, int)
            bits[i] = data.readLong();
        }
        //we can choose better implementation here based on data density
        return new FixedBitSet(bits, length << 6);
    }

    @Override
    public BinaryDocValues getBinary(FieldInfo field) {
        throw new UnsupportedOperationException();
    }

    @Override
    public SortedDocValues getSorted(FieldInfo field) {
        throw new UnsupportedOperationException();
    }

    @Override
    public SortedNumericDocValues getSortedNumeric(FieldInfo field) {
        throw new UnsupportedOperationException();
    }

    @Override
    public SortedSetDocValues getSortedSet(FieldInfo field) {
        throw new UnsupportedOperationException();
    }

    @Override
    public synchronized DocValuesProducer getMergeInstance() {
        return new BooleanDocValuesProducer(this);
    }

    @Override
    public void close() throws IOException {
        data.close();
    }

    static class BooleanEntry {
        long offset;
        int count;
    }

    static class BooleanDocValues extends NumericDocValues {
        private final BitSet bitSet;
        private final int maxDoc;
        private final int cost;
        private int docId = -1;

        BooleanDocValues(BitSet bitSet, int maxDoc) {
            this.bitSet = bitSet;
            this.maxDoc = maxDoc;//performance hint use prevSetBit here
            //is very expensive we can store an information in meta at index time and use as a constant
            this.cost = bitSet.approximateCardinality();
        }

        @Override
        public long longValue() {
            return bitSet.get(docId) ? 1 : 0;
        }

        @Override
        public boolean advanceExact(int target) {
            docId = target;
            return bitSet.get(docId);
        }

        @Override
        public int docID() {
            return docId;
        }

        @Override
        public int nextDoc() {
            return advance(docId + 1);
        }

        @Override
        public int advance(int target) {
            if (target >= maxDoc) {
                docId = NO_MORE_DOCS;
            } else {
                docId = bitSet.nextSetBit(target);
            }
            return docId;
        }

        @Override
        public long cost() {
            return cost;
        }
    }
}
