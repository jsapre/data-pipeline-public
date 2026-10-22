package com.pipeline.ingestion;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

@Service
public class UsgsDataIngestionService {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final RestTemplate restTemplate = new RestTemplate();
    private static final String TOPIC = "usgs-water-metrics";
    private static final String USGS_URL = "https://waterservices.usgs.gov/nwis/iv/?format=json&stateCd=va&parameterCd=00060";

    public UsgsDataIngestionService(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @Scheduled(fixedRate = 60000) // Poll every 60 seconds
    public void ingestTimeSeriesData() {
        try {
            String response = restTemplate.getForObject(USGS_URL, String.class);
            if (response != null) {
                kafkaTemplate.send(TOPIC, "va-water", response);
                System.out.println("Ingested USGS time-series record into Kafka topic: " + TOPIC);
            }
        } catch (Exception e) {
            System.err.println("Failed to fetch USGS data: " + e.getMessage());
        }
    }
}
