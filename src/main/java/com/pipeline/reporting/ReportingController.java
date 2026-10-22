package com.pipeline.reporting;

import com.pipeline.model.WaterMetric;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api/reports")
public class ReportingController {

    private final WaterMetricRepository repository;

    public ReportingController(WaterMetricRepository repository) {
        this.repository = repository;
    }

    @GetMapping("/latest-water-metrics")
    public List<WaterMetric> getLatestWaterMetrics() {
        return repository.findTop10ByOrderByTimestampDesc();
    }
}