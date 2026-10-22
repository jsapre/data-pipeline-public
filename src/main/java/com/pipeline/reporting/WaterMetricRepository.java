package com.pipeline.reporting;

import com.pipeline.model.WaterMetric;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface WaterMetricRepository extends JpaRepository<WaterMetric, Long> {
    List<WaterMetric> findTop10ByOrderByTimestampDesc();
}