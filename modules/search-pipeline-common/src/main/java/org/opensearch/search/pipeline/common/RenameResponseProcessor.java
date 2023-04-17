/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.pipeline.common;

import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.document.DocumentField;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.core.xcontent.MediaType;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.ingest.ConfigurationUtils;
import org.opensearch.search.SearchHit;
import org.opensearch.search.pipeline.Processor;
import org.opensearch.search.pipeline.SearchResponseProcessor;

import java.util.Map;

public class RenameResponseProcessor extends AbstractProcessor implements SearchResponseProcessor{
    private final String oldField;
    private final String newField;
    private final boolean ignoreMissing;

    public static final String TYPE = "rename";

    public RenameResponseProcessor(String tag, String description, String oldField, String newField, boolean ignoreMissing) {
        super(tag, description);
        this.oldField = oldField;
        this.newField = newField;
        this.ignoreMissing = ignoreMissing;
    }

    @Override
    public String getType() { return TYPE; }

    public String getOldField() {
        return oldField;
    }

    public String getNewField() {
        return newField;
    }

    public boolean isIgnoreMissing() {
        return ignoreMissing;
    }

    @Override
    public SearchResponse processResponse(SearchRequest request, SearchResponse response) throws Exception {
        boolean foundField = false;

        SearchHit[] hits = response.getHits().getHits();
        for (int i = 0; i < hits.length; i++) {
            Map<String, DocumentField> fields = hits[i].getFields();
            if (fields.containsKey(oldField)){
                foundField = true;
                DocumentField field = hits[i].removeDocumentField(oldField);
                hits[i].setDocumentField(newField, new DocumentField(newField, field.getValues()));
            }

            BytesReference sourceRef = hits[i].getSourceRef();
            Tuple<? extends MediaType, Map<String, Object>> typeAndSourceMap =
                XContentHelper.convertToMap(sourceRef, false, (MediaType) null);

            Map<String, Object> sourceAsMap = typeAndSourceMap.v2();
            if (sourceAsMap.containsKey(oldField)) {
                foundField = true;
                Object val = sourceAsMap.remove(oldField);
                if (val instanceof DocumentField) {
                    DocumentField dfVal = (DocumentField) val;
                    val = new DocumentField(newField, dfVal.getValues());
                }
                sourceAsMap.put(newField, val);

                XContentBuilder builder = XContentBuilder.builder(typeAndSourceMap.v1().xContent());
                builder.map(sourceAsMap);
                hits[i].sourceRef(BytesReference.bytes(builder));
            }

            if (!foundField && !ignoreMissing) {
                throw new IllegalArgumentException("Document with id " + hits[i].getId() + " is missing field " + oldField);
            }
        }

        return response;
    }

    public static final class Factory implements Processor.Factory {

        final String type = "rename";

        Factory() {}

        @Override
        public RenameResponseProcessor create(Map<String, Processor.Factory> processorFactories, String tag, String description, Map<String, Object> config) throws Exception {
            String oldField = ConfigurationUtils.readStringProperty(TYPE, tag, config, "oldField");
            String newField = ConfigurationUtils.readStringProperty(TYPE, tag, config, "newField");
            boolean ignoreMissing = ConfigurationUtils.readBooleanProperty(TYPE, tag, config, "ignoreMissing", false);
            return new RenameResponseProcessor(tag, description, oldField, newField, ignoreMissing);
        }
    }
}
