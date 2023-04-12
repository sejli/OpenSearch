/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a.java
 * compatible open source license.
 */

package org.opensearch.search.pipeline.common;

import org.apache.lucene.search.TotalHits;
import org.opensearch.OpenSearchParseException;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchResponseSections;
import org.opensearch.common.bytes.BytesArray;
import org.opensearch.common.document.DocumentField;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.ingest.RandomDocumentPicks;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.AbstractBuilderTestCase;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class RenameResponseProcessorTests extends AbstractBuilderTestCase {

    private SearchRequest createDummyRequest() {
        QueryBuilder query = new TermQueryBuilder("field", "value");
        SearchSourceBuilder source = new SearchSourceBuilder().query(query);
        return new SearchRequest().source(source);
    }

    private SearchResponse createTestResponse(int size, boolean includeMapping) {
        SearchHit[] hits = new SearchHit[size];
        for (int i = 0; i < size; i++) {
            Map<String, DocumentField> searchHitFields = new HashMap<>();
            if (includeMapping) {
                searchHitFields.put("field " + i, new DocumentField("value " + i, Collections.emptyList()));
            }
            searchHitFields.put("field " + i, new DocumentField("value " + i, Collections.emptyList()));
            hits[i] = new SearchHit(i, "doc " + i, searchHitFields, Collections.emptyMap());
            hits[i].sourceRef(new BytesArray("{ \"field " + i + "\" : \"value " + i + "\" }"));
            hits[i].score(i);
        }
        SearchHits searchHits = new SearchHits(hits, new TotalHits(size * 2L, TotalHits.Relation.EQUAL_TO), size);
        SearchResponseSections searchResponseSections = new SearchResponseSections(searchHits, null, null, false, false, null, 0);
        return new SearchResponse(searchResponseSections, null, 1, 1, 0, 10, null, null);
    }

    public void testRenameResponse() throws Exception {
        SearchRequest request = createDummyRequest();

        RenameResponseProcessor renameResponseProcessor =
            new RenameResponseProcessor(null, null, "field 0", "new field", false);
        SearchResponse response = createTestResponse(2, false);
        SearchResponse renameResponse = renameResponseProcessor.processResponse(request, createTestResponse(5, false));

        assertNotEquals(response.getHits(), renameResponse.getHits());
    }

    public void testRenameResponseWithMapping() throws Exception {
        SearchRequest request = createDummyRequest();

        RenameResponseProcessor renameResponseProcessor =
            new RenameResponseProcessor(null, null, "field 0", "new field", true);
        SearchResponse response = createTestResponse(5, true);
        SearchResponse renameResponse = renameResponseProcessor.processResponse(request, createTestResponse(5, true));

        assertNotEquals(response.getHits(), renameResponse.getHits());

        boolean foundField = false;
        for (SearchHit hit : renameResponse.getHits().getHits()) {
            if (hit.getFields().containsKey("new field")) {
                foundField = true;
            }
        }
        assertTrue(foundField);
    }

    public void testFactory() throws Exception {
        String oldField = RandomDocumentPicks.randomFieldName(random());
        String newField = RandomDocumentPicks.randomFieldName(random());
        Map<String, Object> config = new HashMap<>();
        config.put("oldField", oldField);
        config.put("newField", newField);

        RenameResponseProcessor.Factory factory = new RenameResponseProcessor.Factory(this.xContentRegistry());
        RenameResponseProcessor processor = factory.create(
            Collections.emptyMap(),
            null,
            null,
            config
        );
        assertEquals(processor.getType(), "rename");
        assertEquals(processor.getOldField(), oldField);
        assertEquals(processor.getNewField(), newField);
        assertFalse(processor.isIgnoreMissing());

        expectThrows(OpenSearchParseException.class, () -> factory.create(Collections.emptyMap(), null, null, Collections.emptyMap()));
    }
}
