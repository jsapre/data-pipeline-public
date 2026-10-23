package com.pipeline.ingestion;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

class UsgsDataIngestionServiceTest {

    private UsgsDataIngestionService service;
    private KafkaTemplate<String, String> kafkaTemplate;

    @BeforeEach
    void setUp() {
        @SuppressWarnings("unchecked")
        KafkaTemplate<String, String> mockTemplate = mock(KafkaTemplate.class);
        this.kafkaTemplate = mockTemplate;
        this.service = new UsgsDataIngestionService(kafkaTemplate);
    }

    @Test
    void testParseTimeSeriesWithValidData() {
        String sampleJson = """
        {
          "name": "ns1:timeSeriesResponseType",
          "value": {
            "timeSeries": [
              {
                "sourceInfo": {
                  "siteName": "JAMES RIVER AT CARTERSVILLE, VA",
                  "siteCode": [{ "value": "02035000", "network": "NWIS" }]
                },
                "variable": {
                  "variableName": "Streamflow, ft^3/s",
                  "noDataValue": -999999.0
                },
                "values": [
                  {
                    "value": [
                      {
                        "value": "2450.0",
                        "dateTime": "2026-10-23T12:00:00.000-04:00"
                      }
                    ]
                  }
                ]
              }
            ]
          }
        }
        """;

        List<Map<String, Object>> metrics = service.parseTimeSeries(sampleJson);

        assertNotNull(metrics);
        assertEquals(1, metrics.size());

        Map<String, Object> metric = metrics.get(0);
        assertEquals("02035000", metric.get("siteCode"));
        assertEquals("02035000", metric.get("site_code"));
        assertEquals("JAMES RIVER AT CARTERSVILLE, VA", metric.get("siteName"));
        assertEquals(2450.0, (Double) metric.get("value"), 0.001);
        assertEquals("2026-10-23T12:00:00.000-04:00", metric.get("timestamp"));
    }

    @Test
    void testParseTimeSeriesExcludesNoDataValues() {
        String sampleJsonWithNoData = """
        {
          "name": "ns1:timeSeriesResponseType",
          "value": {
            "timeSeries": [
              {
                "sourceInfo": {
                  "siteName": "POTOMAC RIVER, VA",
                  "siteCode": [{ "value": "01646500" }]
                },
                "variable": {
                  "variableName": "Streamflow",
                  "noDataValue": -999999.0
                },
                "values": [
                  {
                    "value": [
                      {
                        "value": "-999999.0",
                        "dateTime": "2026-10-23T12:00:00.000-04:00"
                      }
                    ]
                  }
                ]
              }
            ]
          }
        }
        """;

        List<Map<String, Object>> metrics = service.parseTimeSeries(sampleJsonWithNoData);

        assertNotNull(metrics);
        assertTrue(metrics.isEmpty(), "Should exclude sentinel -999999.0 nodata readings");
    }

    @Test
    void testParseTimeSeriesHandlesEmptyOrMalformedJson() {
        List<Map<String, Object>> emptyResult = service.parseTimeSeries("");
        assertNotNull(emptyResult);
        assertTrue(emptyResult.isEmpty());

        List<Map<String, Object>> nullResult = service.parseTimeSeries(null);
        assertNotNull(nullResult);
        assertTrue(nullResult.isEmpty());

        List<Map<String, Object>> invalidJsonResult = service.parseTimeSeries("{ invalid json");
        assertNotNull(invalidJsonResult);
        assertTrue(invalidJsonResult.isEmpty());
    }
}
