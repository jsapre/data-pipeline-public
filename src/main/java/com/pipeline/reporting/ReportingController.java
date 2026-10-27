package com.pipeline.reporting;

import com.pipeline.ingestion.UsgsDataIngestionService;
import com.pipeline.model.WaterMetric;
import com.pipeline.processing.SparkBatchProcessor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/reports")
public class ReportingController {

    private final WaterMetricRepository repository;
    private final SparkBatchProcessor sparkBatchProcessor;
    private final UsgsDataIngestionService ingestionService;

    public ReportingController(WaterMetricRepository repository,
                               SparkBatchProcessor sparkBatchProcessor,
                               UsgsDataIngestionService ingestionService) {
        this.repository = repository;
        this.sparkBatchProcessor = sparkBatchProcessor;
        this.ingestionService = ingestionService;
    }

    @GetMapping("/latest-water-metrics")
    public List<WaterMetric> getLatestWaterMetrics() {
        return repository.findTop10ByOrderByTimestampDesc();
    }

    @GetMapping("/status")
    public ResponseEntity<Map<String, Object>> getPipelineStatus() {
        Map<String, Object> status = new HashMap<>();
        long totalRecords = repository.countNative();
        List<WaterMetric> latest = repository.findTop10ByOrderByTimestampDesc();

        status.put("totalPostgresRecords", totalRecords);
        status.put("latestRecord", latest.isEmpty() ? null : latest.get(0));
        status.put("status", "UP");
        return ResponseEntity.ok(status);
    }

    @PostMapping("/trigger-ingest")
    public ResponseEntity<Map<String, Object>> triggerIngest() {
        int published = ingestionService.ingestTimeSeriesData();
        Map<String, Object> resp = new HashMap<>();
        resp.put("status", "SUCCESS");
        resp.put("recordsPublished", published);
        return ResponseEntity.ok(resp);
    }

    @PostMapping("/sync-to-hdfs")
    public ResponseEntity<Map<String, Object>> syncToHdfs() {
        Map<String, Object> batchResult = sparkBatchProcessor.processKafkaDataToHdfs();
        return ResponseEntity.ok(batchResult);
    }
}
