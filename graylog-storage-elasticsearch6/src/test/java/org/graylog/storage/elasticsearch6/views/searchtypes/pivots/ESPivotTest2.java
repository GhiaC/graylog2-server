package org.graylog.storage.elasticsearch6.views.searchtypes.pivots;

import com.google.common.collect.ImmutableList;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import com.revinate.assertj.json.JsonPathAssert;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.Aggregation;
import io.searchbox.core.search.aggregation.MetricAggregation;
import org.assertj.core.api.AbstractCharSequenceAssert;
import org.graylog.plugins.views.search.Query;
import org.graylog.plugins.views.search.SearchJob;
import org.graylog.plugins.views.search.searchtypes.pivot.BucketSpec;
import org.graylog.plugins.views.search.searchtypes.pivot.Pivot;
import org.graylog.plugins.views.search.searchtypes.pivot.SeriesSpec;
import org.graylog.plugins.views.search.searchtypes.pivot.buckets.AutoInterval;
import org.graylog.plugins.views.search.searchtypes.pivot.buckets.Time;
import org.graylog.plugins.views.search.searchtypes.pivot.buckets.Values;
import org.graylog.plugins.views.search.searchtypes.pivot.series.Count;
import org.graylog.shaded.elasticsearch5.org.elasticsearch.index.query.QueryBuilders;
import org.graylog.shaded.elasticsearch5.org.elasticsearch.search.aggregations.AggregationBuilders;
import org.graylog.shaded.elasticsearch5.org.elasticsearch.search.builder.SearchSourceBuilder;
import org.graylog.storage.elasticsearch6.views.ESGeneratedQueryContext;
import org.graylog.storage.elasticsearch6.views.searchtypes.pivot.ESPivot;
import org.graylog.storage.elasticsearch6.views.searchtypes.pivot.ESPivotBucketSpecHandler;
import org.graylog.storage.elasticsearch6.views.searchtypes.pivot.ESPivotSeriesSpecHandler;
import org.graylog.storage.elasticsearch6.views.searchtypes.pivot.buckets.ESTimeHandler;
import org.graylog.storage.elasticsearch6.views.searchtypes.pivot.buckets.ESValuesHandler;
import org.graylog.storage.elasticsearch6.views.searchtypes.pivot.series.ESCountHandler;
import org.joda.time.DateTimeUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class ESPivotTest2 {
    @Mock
    private SearchJob job;
    @Mock
    private Query query;
    @Mock
    private Pivot pivot;
    @Mock
    private SearchResult queryResult;
    @Mock
    private MetricAggregation aggregations;
    @Mock
    private ESGeneratedQueryContext queryContext;

    private ESPivot esPivot;
    private Map<String, ESPivotBucketSpecHandler<? extends BucketSpec, ? extends Aggregation>> bucketHandlers;
    private Map<String, ESPivotSeriesSpecHandler<? extends SeriesSpec, ? extends Aggregation>> seriesHandlers;

    private AbstractCharSequenceAssert<?, String> extractAggregation(DocumentContext context, String path) {
        final String fullPath = Stream.of(path.split("\\.")).map(s -> "['aggregations']['" + s + "']").reduce("$", (s1, s2) -> s1 + s2) + "['filter']['exists']['field']";
        return JsonPathAssert.assertThat(context).jsonPathAsString(fullPath);
    }

    private void mockBucketSpecGeneratesComparableString(ESPivotBucketSpecHandler<? extends BucketSpec, ? extends Aggregation> bucketHandler) {
        when(bucketHandler.createAggregation(any(), any(), any(), any(), any(), any()))
                .thenAnswer(invocation -> Optional.of(AggregationBuilders.filter(invocation.getArgument(0), QueryBuilders.existsQuery(invocation.getArgument(2).toString()))));
    }

    private void mockSeriesSpecGeneratesComparableString(ESPivotSeriesSpecHandler<? extends SeriesSpec, ? extends Aggregation> seriesHandler) {
        when(seriesHandler.createAggregation(any(), any(), any(), any(), any()))
                .thenAnswer(invocation -> Optional.of(AggregationBuilders.filter(invocation.getArgument(0), QueryBuilders.existsQuery(invocation.getArgument(2).toString()))));
    }

    @BeforeEach
    public void setUp() throws Exception {
        bucketHandlers = new HashMap<>();
        seriesHandlers = new HashMap<>();
        this.esPivot = new ESPivot(bucketHandlers, seriesHandlers);
        when(pivot.id()).thenReturn("dummypivot");
    }

    @AfterEach
    public void tearDown() throws Exception {
        // Some tests modify the time so we make sure to reset it after each test even if assertions fail
        DateTimeUtils.setCurrentMillisSystem();
    }


    @Test
    public void mixedPivotsAndSeriesShouldBeNested() {
        final SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        final ESPivotBucketSpecHandler<? extends BucketSpec, ? extends Aggregation> valuesBucketHandler = mock(ESValuesHandler.class);
        mockBucketSpecGeneratesComparableString(valuesBucketHandler);
        final ESPivotBucketSpecHandler<? extends BucketSpec, ? extends Aggregation> timeBucketHandler = mock(ESTimeHandler.class);
        mockBucketSpecGeneratesComparableString(timeBucketHandler);
        final ESPivotSeriesSpecHandler<? extends SeriesSpec, ? extends Aggregation> countHandler = mock(ESCountHandler.class);
        mockSeriesSpecGeneratesComparableString(countHandler);

        bucketHandlers.put(Values.NAME, valuesBucketHandler);
        bucketHandlers.put(Time.NAME, timeBucketHandler);

        seriesHandlers.put(Count.NAME, countHandler);

        when(queryContext.searchSourceBuilder(pivot)).thenReturn(searchSourceBuilder);
        when(queryContext.nextName()).thenReturn("rowPivot1", "rowPivot2", "columnPivot1", "columnPivot2");

        final BucketSpec rowPivot1 = Time.builder().field("timestamp").interval(AutoInterval.create()).build();
        final BucketSpec rowPivot2 = Values.builder().field("http_method").limit(10).build();
        final BucketSpec columnPivot1 = Values.builder().field("controller").limit(10).build();
        final BucketSpec columnPivot2 = Values.builder().field("action").limit(10).build();
        final Count count = Count.builder().build();
        when(pivot.rowGroups()).thenReturn(ImmutableList.of(rowPivot1, rowPivot2));
        when(pivot.columnGroups()).thenReturn(ImmutableList.of(columnPivot1, columnPivot2));
        when(pivot.series()).thenReturn(Collections.singletonList(count));
        when(pivot.rollup()).thenReturn(false);
        when(queryContext.seriesName(any(), any())).thenCallRealMethod();

        this.esPivot.doGenerateQueryPart(job, query, pivot, queryContext);

//        verify(timeBucketHandler).createAggregation(eq("rowPivot1"), eq(pivot), eq(rowPivot1), eq(this.esPivot), eq(queryContext), eq(query));
//        verify(valuesBucketHandler).createAggregation(eq("rowPivot2"), eq(pivot), eq(rowPivot2), eq(this.esPivot), eq(queryContext), eq(query));
//        verify(valuesBucketHandler).createAggregation(eq("columnPivot1"), eq(pivot), eq(columnPivot1), eq(this.esPivot), eq(queryContext), eq(query));
//        verify(valuesBucketHandler).createAggregation(eq("columnPivot2"), eq(pivot), eq(columnPivot2), eq(this.esPivot), eq(queryContext), eq(query));

        final DocumentContext context = JsonPath.parse(searchSourceBuilder.toString());
        extractAggregation(context, "rowPivot1")
                .isEqualTo("Time{type=time, field=timestamp, interval=AutoInterval{type=auto, scaling=1.0}}");
        extractAggregation(context, "rowPivot1.rowPivot2")
                .isEqualTo("Values{type=values, field=http_method, limit=10}");
        extractAggregation(context, "rowPivot1.rowPivot2.columnPivot1")
                .isEqualTo("Values{type=values, field=controller, limit=10}");
        extractAggregation(context, "rowPivot1.rowPivot2.columnPivot1.columnPivot2")
                .isEqualTo("Values{type=values, field=action, limit=10}");
        extractAggregation(context, "rowPivot1.rowPivot2.dummypivot-series-count()")
                .isEqualTo("Count{type=count, id=count(), field=null}");
        extractAggregation(context, "rowPivot1.rowPivot2.columnPivot1.columnPivot2.dummypivot-series-count()")
                .isEqualTo("Count{type=count, id=count(), field=null}");
    }

}
