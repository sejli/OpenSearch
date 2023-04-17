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

                                                                                                                           import java.util.Arrays;
                                                                                                                           import java.util.Collections;
import java.util.List;
import java.util.Map;

public class RenameResponseValuesProcessor extends AbstractProcessor implements SearchResponseProcessor {
    private final List<String> targets;

    public static final String TYPE = "rename_values";

    public RenameResponseValuesProcessor(String tag, String description, List<String> targets) {
        super(tag, description);
        this.targets = targets;
    }

    @Override
    public String getType() { return TYPE; }

    public List<String> getTargets() { return targets; }

    @Override
    public SearchResponse processResponse(SearchRequest request, SearchResponse response) throws Exception{
        System.out.println(targets.toString());

        SearchHit [] hits = response.getHits().getHits();
        for (int i = 0; i < hits.length; i++ ) {
            Map<String, DocumentField> fields = hits[i].getFields();
            for (String key : fields.keySet()) {
                DocumentField field = fields.get(key);
                if (field.getValues().size() == 1) {
                    String stringValue = field.getValues().get(0).toString();
                    for (String word : targets) {
                        if (stringValue.contains(word)) {
                            String replacement = "*".repeat(word.length());
                            stringValue = stringValue.replace(word, replacement);
                        }
                    }
                    hits[i].removeDocumentField(key);
                    hits[i].setDocumentField(key, new DocumentField(key, List.of(stringValue)));
                }
            }
            BytesReference sourceRef = hits[i].getSourceRef();
            Tuple<? extends MediaType, Map<String, Object>> typeAndSourceMap =
                XContentHelper.convertToMap(sourceRef, false, (MediaType) null);

            Map<String, Object> sourceAsMap = typeAndSourceMap.v2();
            for (String key: sourceAsMap.keySet()) {
                Object val = sourceAsMap.remove(key);
                if (val instanceof DocumentField) {
                    DocumentField field = (DocumentField) val;
                    if (field.getValues().size() == 1) {
                        String stringValue = field.getValues().get(0).toString();
                        for (String word : targets) {
                            if (stringValue.contains(word)) {
                                String replacement = "*".repeat(word.length());
                                stringValue = stringValue.replace(word, replacement);
                            }
                        }
                        sourceAsMap.put(key, new DocumentField(key, List.of(stringValue)));
                    }

                }
                else {
                    String stringValue = val.toString();
                    for (String word : targets) {
                        if (stringValue.contains(word)) {
                            String replacement = "*".repeat(word.length());
                            stringValue = stringValue.replace(word, replacement);
                        }
                    }
                    System.out.println(stringValue);
                    sourceAsMap.put(key, stringValue);
                }
                XContentBuilder builder = XContentBuilder.builder(typeAndSourceMap.v1().xContent());
                builder.map(sourceAsMap);
                hits[i].sourceRef(BytesReference.bytes(builder));
            }
        }
        return response;
    }

    public static final class Factory implements Processor.Factory {
        final String type = "rename_values";

        Factory() {}

        @Override
        public RenameResponseValuesProcessor create(Map<String, Processor.Factory> processorFactories, String tag, String description, Map<String, Object> config) throws Exception {
            String targetsString = ConfigurationUtils.readStringProperty(TYPE, tag, config, "targets");
            List<String> targets = Arrays.asList(targetsString.split(" "));
            return new RenameResponseValuesProcessor(tag, description, targets);
        }

    }
}
