package com.pipeline.ingestion;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pipeline.config.KafkaConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class UsgsDataIngestionService {

    private static final Logger log = LoggerFactory.getLogger(UsgsDataIngestionService.class);
    private static final String USGS_URL = "https://waterservices.usgs.gov/nwis/iv/?format=json&stateCd=va&parameterCd=00060";

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final RestTemplate restTemplate;
    private final ObjectMapper objectMapper;

    public UsgsDataIngestionService(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
        this.restTemplate = new RestTemplate();
        this.objectMapper = new ObjectMapper();
    }

    public UsgsDataIngestionService(KafkaTemplate<String, String> kafkaTemplate, RestTemplate restTemplate, ObjectMapper objectMapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.restTemplate = restTemplate;
        this.objectMapper = objectMapper;
    }

    @Scheduled(fixedRate = 60000, initialDelay = 10000) // Poll every 60 seconds
    public int ingestTimeSeriesData() {
        try {
            log.info("Initiating scheduled fetch from USGS NWIS: {}", USGS_URL);
            String response = restTemplate.getForObject(USGS_URL, String.class);
            if (response == null || response.trim().isEmpty()) {
                log.warn("Empty response received from USGS NWIS API");
                return 0;
            }

            List<Map<String, Object>> metrics = parseTimeSeries(response);
            int publishedCount = 0;

            for (Map<String, Object> metric : metrics) {
                String siteCode = (String) metric.get("siteCode");
                String payload = objectMapper.writeValueAsString(metric);
                kafkaTemplate.send(KafkaConfig.USGS_TOPIC, siteCode, payload);
                publishedCount++;
            }

            log.info("Published {} USGS water metric records to Kafka topic '{}'", publishedCount, KafkaConfig.USGS_TOPIC);
            return publishedCount;
        } catch (Exception e) {
            log.error("Failed to fetch or publish USGS data: {}", e.getMessage(), e);
            return 0;
        }
    }

    /**
     * Parses the nested WaterML 2.0 / NWIS JSON payload and extracts flattened individual station metrics.
     */
    public List<Map<String, Object>> parseTimeSeries(String jsonResponse) {
        List<Map<String, Object>> result = new ArrayList<>();
        if (jsonResponse == null || jsonResponse.trim().isEmpty()) {
            return result;
        }

        try {
            JsonNode rootNode = objectMapper.readTree(jsonResponse);
            JsonNode timeSeriesList = rootNode.path("value").path("timeSeries");

            if (!timeSeriesList.isArray()) {
                return result;
            }

            for (JsonNode tsNode : timeSeriesList) {
                JsonNode sourceInfo = tsNode.path("sourceInfo");
                String siteCode = sourceInfo.path("siteCode").path(0).path("value").asText(null);
                String siteName = sourceInfo.path("siteName").asText(null);

                JsonNode variableNode = tsNode.path("variable");
                double noDataValue = variableNode.path("noDataValue").asDouble(-999999.0);
                String variableName = variableNode.path("variableName").asText("Streamflow");

                JsonNode valuesArray = tsNode.path("values").path(0).path("value");
                if (valuesArray.isArray() && valuesArray.size() > 0) {
                    // Pick the most recent reading in the array
                    JsonNode latestReading = valuesArray.path(valuesArray.size() - 1);
                    String valStr = latestReading.path("value").asText(null);
                    String dateTimeStr = latestReading.path("dateTime").asText(null);

                    if (valStr != null && !valStr.trim().isEmpty() && dateTimeStr != null && siteCode != null) {
                        try {
                            double numericVal = Double.parseDouble(valStr);
                            // Exclude USGS sentinel nodata values (e.g. -999999.0)
                            if (Math.abs(numericVal - noDataValue) > 0.01) {
                                Map<String, Object> metric = new HashMap<>();
                                metric.put("siteCode", siteCode);
                                metric.put("site_code", siteCode);
                                metric.put("siteName", siteName);
                                metric.put("variable", variableName);
                                metric.put("timestamp", dateTimeStr);
                                metric.put("dateTime", dateTimeStr);
                                metric.put("value", numericVal);
                                result.add(metric);
                            }
                        } catch (NumberFormatException nfe) {
                            log.debug("Skipping non-numeric value '{}' for site {}", valStr, siteCode);
                        }
                    }
                }
            }
        } catch (Exception e) {
            log.error("Error parsing USGS WaterML JSON response: {}", e.getMessage(), e);
        }

        return result;
    }
}
