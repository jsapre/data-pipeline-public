package com.pipeline.reporting;

import com.pipeline.model.WaterMetric;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface WaterMetricRepository extends JpaRepository<WaterMetric, Long> {

    @Query(value = "SELECT * FROM water_metrics ORDER BY timestamp DESC LIMIT 10", nativeQuery = true)
    List<WaterMetric> findTop10ByOrderByTimestampDesc();

    @Query(value = "SELECT COUNT(*) FROM water_metrics", nativeQuery = true)
    long countNative();
}
