/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.percolate;

import org.apache.lucene.util.UnicodeUtil;
import org.elasticsearch.ElasticSearchGenerationException;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.single.custom.SingleCustomOperationRequest;
import org.elasticsearch.common.Required;
import org.elasticsearch.common.Unicode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

import static org.elasticsearch.action.Actions.*;

/**
 * @author kimchy
 */
public class PercolateRequest extends SingleCustomOperationRequest {

    private String index;

    private byte[] source;
    private int sourceOffset;
    private int sourceLength;
    private boolean sourceUnsafe;

    PercolateRequest() {

    }

    /**
     * Constructs a new percolate request.
     *
     * @param index The index name
     */
    public PercolateRequest(String index) {
        this.index = index;
    }

    public PercolateRequest index(String index) {
        this.index = index;
        return this;
    }

    public String index() {
        return this.index;
    }

    /**
     * Before we fork on a local thread, make sure we copy over the bytes if they are unsafe
     */
    @Override public void beforeLocalFork() {
        source();
    }

    public byte[] source() {
        if (sourceUnsafe || sourceOffset > 0) {
            source = Arrays.copyOfRange(source, sourceOffset, sourceOffset + sourceLength);
            sourceOffset = 0;
            sourceUnsafe = false;
        }
        return source;
    }

    public byte[] unsafeSource() {
        return this.source;
    }

    public int unsafeSourceOffset() {
        return this.sourceOffset;
    }

    public int unsafeSourceLength() {
        return this.sourceLength;
    }

    @Required public PercolateRequest source(Map source) throws ElasticSearchGenerationException {
        return source(source, XContentType.SMILE);
    }

    @Required public PercolateRequest source(Map source, XContentType contentType) throws ElasticSearchGenerationException {
        try {
            XContentBuilder builder = XContentFactory.contentBuilder(contentType);
            builder.map(source);
            return source(builder);
        } catch (IOException e) {
            throw new ElasticSearchGenerationException("Failed to generate [" + source + "]", e);
        }
    }

    @Required public PercolateRequest source(String source) {
        UnicodeUtil.UTF8Result result = Unicode.fromStringAsUtf8(source);
        this.source = result.result;
        this.sourceOffset = 0;
        this.sourceLength = result.length;
        this.sourceUnsafe = true;
        return this;
    }

    @Required public PercolateRequest source(XContentBuilder sourceBuilder) {
        try {
            source = sourceBuilder.unsafeBytes();
            sourceOffset = 0;
            sourceLength = sourceBuilder.unsafeBytesLength();
            sourceUnsafe = true;
        } catch (IOException e) {
            throw new ElasticSearchGenerationException("Failed to generate [" + sourceBuilder + "]", e);
        }
        return this;
    }

    public PercolateRequest source(byte[] source) {
        return source(source, 0, source.length);
    }

    @Required public PercolateRequest source(byte[] source, int offset, int length) {
        return source(source, offset, length, false);
    }

    @Required public PercolateRequest source(byte[] source, int offset, int length, boolean unsafe) {
        this.source = source;
        this.sourceOffset = offset;
        this.sourceLength = length;
        this.sourceUnsafe = unsafe;
        return this;
    }

    @Override public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = super.validate();
        if (index == null) {
            validationException = addValidationError("index is missing", validationException);
        }
        if (source == null) {
            validationException = addValidationError("source is missing", validationException);
        }
        return validationException;
    }

    @Override public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        index = in.readUTF();

        sourceUnsafe = false;
        sourceOffset = 0;
        sourceLength = in.readVInt();
        source = new byte[sourceLength];
        in.readFully(source);
    }

    @Override public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeUTF(index);

        out.writeVInt(sourceLength);
        out.writeBytes(source, sourceOffset, sourceLength);
    }
}
