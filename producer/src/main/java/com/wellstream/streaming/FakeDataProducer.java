package com.wellstream.streaming;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.codec.binary.Base64;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.javafaker.Faker;

import redis.clients.jedis.Jedis;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

/**
 * STEP 1-2: Kafka Producer generating fake Well and Tank sensor data
 * Simulates IoT devices sending real-time data to Kafka topics
 * 
 * - Well data = energy asset metrics (like transaction attributes)
 * - Tank data = facility state (like account or product balance)
 */
public class FakeDataProducer {
    
    public static void main(String[] args) throws Exception {
        Faker faker = new Faker();
        ObjectMapper mapper = new ObjectMapper();

        // Redis client
        String redisHost = System.getenv("REDIS_HOST");
        if (redisHost == null) {
            redisHost = "localhost";
        }
        Jedis jedis = new Jedis(redisHost, 6379);
        System.out.println("✓ Connected to Redis: " + redisHost);


        // Kafka Producer Config
        Properties props = new Properties();

        // Detect environment: Docker vs Local
        String bootstrapServers = System.getenv("KAFKA_BOOTSTRAP_SERVERS");
        if (bootstrapServers == null) {
            bootstrapServers = "localhost:9092";  // Local development
        }

        props.put("bootstrap.servers", bootstrapServers);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", KafkaAvroSerializer.class.getName());
        props.put("acks", "all");  // All replicas must acknowledge
        props.put("retries", "3");
        props.put("max.in.flight.requests.per.connection", "1");  // Order guarantee
        
        // Schema Registry URL
        String schemaRegistryUrl = System.getenv("SCHEMA_REGISTRY_URL");
        if (schemaRegistryUrl == null) {
            schemaRegistryUrl = "http://localhost:8081";
        }
        props.put("schema.registry.url", schemaRegistryUrl);

        KafkaProducer<String, Object> producer = new KafkaProducer<>(props);

        System.out.println("✓ Connected to Kafka: " + bootstrapServers);
        System.out.println("✓ Using Schema Registry: " + schemaRegistryUrl);
        
        // Define Avro schemas
        String wellSchemaString = "{"
            + "\"type\": \"record\","
            + "\"name\": \"WellData\","
            + "\"namespace\": \"com.wellstream.streaming\","
            + "\"fields\": ["
            + "  {\"name\": \"well_id\", \"type\": \"string\"},"
            + "  {\"name\": \"facility_id\", \"type\": \"string\"},"
            + "  {\"name\": \"timestamp\", \"type\": \"long\"},"
            + "  {\"name\": \"methane_ppm\", \"type\": \"double\"},"
            + "  {\"name\": \"co2_ppm\", \"type\": \"double\"},"
            + "  {\"name\": \"pressure_psi\", \"type\": \"double\"},"
            + "  {\"name\": \"temperature_f\", \"type\": \"double\"},"
            + "  {\"name\": \"flow_rate_bpd\", \"type\": \"double\"},"
            + "  {\"name\": \"vibration_encoded\", \"type\": \"string\"},"
            + "  {\"name\": \"pump_status\", \"type\": \"string\"},"
            + "  {\"name\": \"maintenance_alert\", \"type\": \"boolean\"}"
            + "]}";

        String tankSchemaString = "{"
            + "\"type\": \"record\","
            + "\"name\": \"TankData\","
            + "\"namespace\": \"com.wellstream.streaming\","
            + "\"fields\": ["
            + "  {\"name\": \"tank_id\", \"type\": \"string\"},"
            + "  {\"name\": \"facility_id\", \"type\": \"string\"},"
            + "  {\"name\": \"timestamp\", \"type\": \"long\"},"
            + "  {\"name\": \"level_percentage\", \"type\": \"double\"},"
            + "  {\"name\": \"vapor_concentration_ppm\", \"type\": \"double\"},"
            + "  {\"name\": \"leak_detection_status\", \"type\": \"string\"},"
            + "  {\"name\": \"ambient_temperature_f\", \"type\": \"double\"},"
            + "  {\"name\": \"internal_pressure_psi\", \"type\": \"double\"},"
            + "  {\"name\": \"flame_arrestor_status\", \"type\": \"boolean\"},"
            + "  {\"name\": \"vapor_recovery_active\", \"type\": \"boolean\"},"
            + "  {\"name\": \"emergency_shutdown\", \"type\": \"boolean\"},"
            + "  {\"name\": \"wind_speed_mph\", \"type\": \"double\"},"
            + "  {\"name\": \"atmospheric_pressure_mb\", \"type\": \"double\"},"
            + "  {\"name\": \"air_quality_index\", \"type\": \"int\"}"
            + "]}";

        Schema wellSchema = new Schema.Parser().parse(wellSchemaString);
        Schema tankSchema = new Schema.Parser().parse(tankSchemaString);
        
        // Thread pool for concurrent producers
        ExecutorService executor = Executors.newFixedThreadPool(2);
        
        // Shared state for correlation
        class SharedFacilityState {
            String facilityId;
            long lastWellTimestamp;
            long lastTankTimestamp;
        }

        java.util.Map<String, SharedFacilityState> facilityStates = new java.util.concurrent.ConcurrentHashMap<>();
        
        // Initialize facilities with correlated data
        for (int i = 1; i <= 10; i++) {
            SharedFacilityState state = new SharedFacilityState();
            state.facilityId = "FAC-" + i;
            state.lastWellTimestamp = System.currentTimeMillis();
            state.lastTankTimestamp = System.currentTimeMillis();
            facilityStates.put(state.facilityId, state);
        }

        // Well Data Producer Thread
        executor.submit(() -> {
            System.out.println("Starting Well Data Producer...");
            int[] wellCount = {0}; 
            while (true) {
                try {
                    // Use fixed facilities for correlation
                    String facilityId = "FAC-" + (wellCount[0] % 10 + 1);
                    String wellId = "WELL-" + (wellCount[0] % 20 + 1);

                    // Create Avro GenericRecord for well data
                    GenericRecord wellRecord = new GenericData.Record(wellSchema);
                    wellRecord.put("well_id", wellId);
                    wellRecord.put("facility_id", facilityId);
                    wellRecord.put("timestamp", System.currentTimeMillis());
                    wellRecord.put("methane_ppm", faker.number().randomDouble(2, 5, 800));
                    wellRecord.put("co2_ppm", faker.number().randomDouble(2, 10, 1000));
                    wellRecord.put("pressure_psi", faker.number().randomDouble(2, 100, 500));
                    wellRecord.put("temperature_f", faker.number().randomDouble(2, 50, 130));
                    wellRecord.put("flow_rate_bpd", faker.number().randomDouble(2, 50, 300));
                    wellRecord.put("vibration_encoded", Base64.encodeBase64String(faker.lorem().sentence().getBytes()));
                    wellRecord.put("pump_status", faker.options().option("active", "inactive", "standby"));
                    wellRecord.put("maintenance_alert", faker.bool().bool());

                    producer.send(new ProducerRecord<>("well-sensor-data", wellId, wellRecord));
                    wellCount[0]++;
                    System.out.println("Produced Well Event " + wellCount[0] + ": " + wellId);
                    Thread.sleep(1000); // Simulate 1-second interval

                } catch (Exception e) {
                    System.err.println("Error in well producer: " + e.getMessage());
                    e.printStackTrace();
                }
            }
        });

        // Tank Data Producer Thread
        executor.submit(() -> {
            System.out.println("Starting Tank Data Producer...");
            int[] tankCount = {0};
            while (true) {
                try {
                     // Use same facilities as wells for correlation
                    String facilityId = "FAC-" + (tankCount[0] % 10 + 1);
                    String tankId = "TANK-" + (tankCount[0] % 15 + 1);

                    // Create Avro GenericRecord for tank data
                    GenericRecord tankRecord = new GenericData.Record(tankSchema);
                    tankRecord.put("tank_id", tankId);
                    tankRecord.put("facility_id", facilityId);
                    tankRecord.put("timestamp", System.currentTimeMillis());
                    tankRecord.put("level_percentage", faker.number().randomDouble(2, 10, 95));
                    tankRecord.put("vapor_concentration_ppm", faker.number().randomDouble(2, 100, 15000));
                    tankRecord.put("leak_detection_status", faker.options().option("NORMAL", "LEAK_DETECTED", "CRITICAL"));
                    tankRecord.put("ambient_temperature_f", faker.number().randomDouble(2, 40, 110));
                    tankRecord.put("internal_pressure_psi", faker.number().randomDouble(2, 5, 50));
                    tankRecord.put("flame_arrestor_status", faker.bool().bool());
                    tankRecord.put("vapor_recovery_active", faker.bool().bool());
                    tankRecord.put("emergency_shutdown", faker.bool().bool());
                    tankRecord.put("wind_speed_mph", faker.number().randomDouble(2, 0, 30));
                    tankRecord.put("atmospheric_pressure_mb", faker.number().randomDouble(2, 950, 1050));
                    tankRecord.put("air_quality_index", faker.number().numberBetween(0, 500));

                    long eventTimestamp = (long) tankRecord.get("timestamp");
                    
                    producer.send(new ProducerRecord<>("tank-sensor-data", tankId, tankRecord),
                        (metadata, exception) -> {
                            if (exception != null) {
                                System.err.println("❌ Tank produce error: " + exception.getMessage());
                            } else if (tankCount[0] % 5 == 0) {
                                System.out.println("✓ Tank event #" + tankCount[0] + " [" + facilityId + "] sent");
                            }
                        }
                    );
                    
                    facilityStates.get(facilityId).lastTankTimestamp = eventTimestamp;
                    tankCount[0]++;
                    Thread.sleep(1000);  // Tank every 1000ms (slightly offset)

                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });

        // Keep producer alive
        executor.awaitTermination(Long.MAX_VALUE, java.util.concurrent.TimeUnit.DAYS);
    }
}