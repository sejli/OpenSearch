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
import java.util.List;
import java.util.Map;

public class RenameResponseValuesProcessorTests extends AbstractBuilderTestCase {
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
            hits[i].sourceRef(new BytesArray("{ \"field " + i + "\" : \"value " + "poop " + i + "\" }"));
            hits[i].score(i);
        }
        SearchHits searchHits = new SearchHits(hits, new TotalHits(size * 2L, TotalHits.Relation.EQUAL_TO), size);
        SearchResponseSections searchResponseSections = new SearchResponseSections(searchHits, null, null, false, false, null, 0);
        return new SearchResponse(searchResponseSections, null, 1, 1, 0, 10, null, null);
    }

    public void testRenameResponseValues() throws Exception {
        SearchRequest request = createDummyRequest();

        RenameResponseValuesProcessor processor =
            new RenameResponseValuesProcessor(null, null, List.of("poop"));
        SearchResponse response = createTestResponse(1, false);
        SearchResponse responseResult = processor.processResponse(request, createTestResponse(1, false));

        assertNotEquals(response.getHits(), responseResult.getHits());
    }
}
