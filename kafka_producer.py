import json
from kafka import KafkaAdminClient, KafkaConsumer, KafkaProducer
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError

BOOTSTRAP = "kafka:9092"
TOPIC = "__STUDENT_TOPIC__"

# 1. Khởi tạo Topic an toàn với 1 partition để cấp phát Leader tức thì
admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP, client_id="student-lab")
try:
    admin.create_topics([NewTopic(TOPIC, num_partitions=1, replication_factor=1)])
except TopicAlreadyExistsError:
    pass
finally:
    admin.close()

# 2. Dữ liệu mô phỏng sự kiện thời gian thực
events = [
    {"timestamp": "2026-10-28 20:01:00", "driver_id": "DRV_101", "vehicle_speed": 75.5, "eye_aspect_ratio": 0.29, "mouth_opening_ratio": 0.08, "head_pitch": 1.5, "is_drowsy": 0},
    {"timestamp": "2026-10-28 20:01:02", "driver_id": "DRV_102", "vehicle_speed": 82.0, "eye_aspect_ratio": 0.17, "mouth_opening_ratio": 0.65, "head_pitch": 19.2, "is_drowsy": 1},
    {"timestamp": "2026-10-28 20:01:04", "driver_id": "DRV_103", "vehicle_speed": 64.0, "eye_aspect_ratio": 0.28, "mouth_opening_ratio": 0.05, "head_pitch": -0.5, "is_drowsy": 0},
    {"timestamp": "2026-10-28 20:01:06", "driver_id": "DRV_102", "vehicle_speed": 84.5, "eye_aspect_ratio": 0.15, "mouth_opening_ratio": 0.72, "head_pitch": 21.0, "is_drowsy": 1},
    {"timestamp": "2026-10-28 20:01:08", "driver_id": "DRV_101", "vehicle_speed": 70.0, "eye_aspect_ratio": 0.31, "mouth_opening_ratio": 0.02, "head_pitch": 0.8, "is_drowsy": 0}
]

# 3. Kafka Producer cấu hình retries
producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP,
    retries=5,
    acks='all',
    key_serializer=lambda value: value.encode("utf-8"),
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
)

for event in events:
    producer.send(TOPIC, key=event["driver_id"], value=event)
    print("Kafka Sent Event:", event)

producer.flush()
producer.close()

# 4. Kafka Consumer đọc và kiểm tra thông điệp
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BOOTSTRAP,
    group_id="driver-safety-monitor-group",
    auto_offset_reset="earliest",
    enable_auto_commit=False,
    consumer_timeout_ms=5000,
    value_deserializer=lambda raw: json.loads(raw.decode("utf-8")),
)

print("\n=== KẾT QUẢ ĐỌC TỪ KAFKA CONSUMER ===")
for message in consumer:
    print(f"partition={message.partition} offset={message.offset} value={message.value}")
consumer.close()