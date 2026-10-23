package com.pipeline.reporting;

import com.pipeline.ingestion.UsgsDataIngestionService;
import com.pipeline.model.WaterMetric;
import com.pipeline.processing.SparkBatchProcessor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.ResponseEntity;

import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class ReportingControllerTest {

    private WaterMetricRepository repository;
    private SparkBatchProcessor sparkBatchProcessor;
    private UsgsDataIngestionService ingestionService;
    private ReportingController controller;

    @BeforeEach
    void setUp() {
        this.repository = mock(WaterMetricRepository.class);
        this.sparkBatchProcessor = mock(SparkBatchProcessor.class);
        this.ingestionService = mock(UsgsDataIngestionService.class);
        this.controller = new ReportingController(repository, sparkBatchProcessor, ingestionService);
    }

    @Test
    void testGetLatestWaterMetrics() {
        WaterMetric metric = new WaterMetric("02035000", 1250.5, LocalDateTime.now());
        when(repository.findTop10ByOrderByTimestampDesc()).thenReturn(List.of(metric));

        List<WaterMetric> result = controller.getLatestWaterMetrics();

        assertNotNull(result);
        assertEquals(1, result.size());
        assertEquals("02035000", result.get(0).getSiteCode());
        assertEquals(1250.5, result.get(0).getValue());
    }

    @Test
    void testGetPipelineStatus() {
        when(repository.count()).thenReturn(42L);
        when(repository.findTop10ByOrderByTimestampDesc()).thenReturn(Collections.emptyList());

        ResponseEntity<Map<String, Object>> response = controller.getPipelineStatus();

        assertNotNull(response);
        assertEquals(200, response.getStatusCode().value());
        assertEquals(42L, response.getBody().get("totalPostgresRecords"));
        assertEquals("UP", response.getBody().get("status"));
    }

    @Test
    void testTriggerIngest() {
        when(ingestionService.ingestTimeSeriesData()).thenReturn(150);

        ResponseEntity<Map<String, Object>> response = controller.triggerIngest();

        assertNotNull(response);
        assertEquals(200, response.getStatusCode().value());
        assertEquals(150, response.getBody().get("recordsPublished"));
        assertEquals("SUCCESS", response.getBody().get("status"));
    }

    @Test
    void testSyncToHdfs() {
        Map<String, Object> batchResult = Map.of("status", "SUCCESS", "recordsProcessed", 200L);
        when(sparkBatchProcessor.processKafkaDataToHdfs()).thenReturn(batchResult);

        ResponseEntity<Map<String, Object>> response = controller.syncToHdfs();

        assertNotNull(response);
        assertEquals(200, response.getStatusCode().value());
        assertEquals("SUCCESS", response.getBody().get("status"));
    }
}
