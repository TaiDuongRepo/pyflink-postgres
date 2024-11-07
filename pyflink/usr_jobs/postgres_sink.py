import json
import logging
from datetime import datetime
import uuid

from pyflink.common import Row, Types, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import JdbcSink
from pyflink.datastream.connectors.jdbc import (
    JdbcConnectionOptions,
    JdbcExecutionOptions,
)
from pyflink.datastream.connectors.kafka import (
    DeliveryGuarantee,
    KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema,
    KafkaSink,
    KafkaSource,
)
from transformers import pipeline

KAFKA_HOST = "kafka:19092"
POSTGRES_HOST = "postgres:5432"

def parse_date_data(data: str) -> Row:
    data = json.loads(data)
    id = uuid.uuid4().hex
    created_at = data["created_at"]
    full_date = datetime.strptime(created_at, "%Y-%m-%dT%H:%M:%SZ").date()
    weekday = datetime.strptime(created_at, "%Y-%m-%dT%H:%M:%SZ").strftime("%A")
    weekmonth = (datetime.strptime(created_at, "%Y-%m-%dT%H:%M:%SZ").day - 1) // 7 + 1
    day = datetime.strptime(created_at, "%Y-%m-%dT%H:%M:%SZ").day
    month = datetime.strptime(created_at, "%Y-%m-%dT%H:%M:%SZ").month
    year = datetime.strptime(created_at, "%Y-%m-%dT%H:%M:%SZ").year
    quarter = (datetime.strptime(created_at, "%Y-%m-%dT%H:%M:%SZ").month - 1) // 3 + 1
    return Row(id, full_date, weekday, weekmonth, day, month, year, quarter)

def parse_time_data(data: str) -> Row:
    data = json.loads(data)
    id = uuid.uuid4().hex
    full_time = data["created_at"]
    hour = datetime.strptime(full_time, "%Y-%m-%dT%H:%M:%SZ").hour
    minute = datetime.strptime(full_time, "%Y-%m-%dT%H:%M:%SZ").minute
    second = datetime.strptime(full_time, "%Y-%m-%dT%H:%M:%SZ").second
    am_pm = "AM" if hour < 12 else "PM"
    shift = "Morning" if 4 <= hour < 12 else "Afternoon" if 12 <= hour < 20 else "Night"
    return Row(id, full_time, hour, minute, second, am_pm, shift)

def parse_fact_content_data(data: str) -> Row:
    data = json.loads(data)
    id = uuid.uuid4().hex
    sentiment_analyzer = pipeline("text-classification", model="5CD-AI/Vietnamese-Sentiment-visobert")
    sentiment_score = sentiment_analyzer(data["content"])[0]["score"]
    post_id = data["post_id"]
    comment_id = data["comment_id"]
    return Row(id, sentiment_score, post_id, comment_id)

def parse_social_data(data: str) -> Row:
    data = json.loads(data)
    id = data["id"]
    name = data["social_type"]
    brand = data["brand"]
    return Row(id, name, brand)

def parse_user_data(data: str) -> Row:
    data = json.loads(data)
    id = uuid.uuid4().hex
    user_id = data["user_id"]
    name = data["user_name"]
    handle = ""
    email = data["email"]
    phone = data["phone"]
    address = data["address"]
    return Row(id, user_id, name, handle, email, phone, address)

def parse_post_data(data: str) -> Row:
    data = json.loads(data)
    id = uuid.uuid4().hex
    post_id = data["id"]
    user_id = data["user_id"]
    social_id = 1
    created_at = data["created_at"]
    created_date = datetime.strptime(created_at, "%Y-%m-%dT%H:%M:%SZ").date()
    created_time = datetime.strptime(created_at, "%Y-%m-%dT%H:%M:%SZ").time()
    url = data["url"]
    title = data["title"]
    content = data["content"]
    sentiment_score = 0.0
    count_comment = data["count_comment"]
    count_reaction = 0
    count_like = 0
    count_dislike = 0
    count_view = data["view"]
    return Row(id, post_id, user_id, social_id, created_date, created_time, url, title, content, sentiment_score, count_comment, count_reaction, count_like, count_dislike, count_view)

def parse_comment_data(data: str) -> Row:
    data = json.loads(data)
    comments = data["comments"]
    for comment in comments:
        id = uuid.uuid4().hex
        comment_id = comment["id"]
        user_id = comment["user_id"]
        created_date = datetime.strptime(comment["created_at"], "%Y-%m-%dT%H:%M:%SZ").date()
        created_time = datetime.strptime(comment["created_at"], "%Y-%m-%dT%H:%M:%SZ").time()
        url = ""
        content = comment["content"]
        sentiment_score = 0
        count_react = 0
        count_like = comment["reaction"]["like"]
        count_dislike = 0
        yield Row(id, comment_id, user_id, created_date, created_time, url, content, sentiment_score, count_react, count_like, count_dislike)

def parse_hashtag_data(data: str) -> Row:
    data = json.loads(data)
    id = uuid.uuid4().hex
    hashtags = data["keyword"][0]
    return Row(id, hashtags)

def parse_fact_posttag_data(data: str) -> Row:
    data = json.loads(data)
    id = uuid.uuid4().hex
    post_id = data["post_id"]
    hashtag_id = data["hashtag_id"]
    return Row(id, post_id, hashtag_id)




def initialize_env() -> StreamExecutionEnvironment:
    """Makes stream execution environment initialization"""
    env = StreamExecutionEnvironment.get_execution_environment()

    # Get current directory
    root_dir_list = __file__.split("/")[:-2]
    root_dir = "/".join(root_dir_list)

    # Adding the jar to the flink streaming environment
    env.add_jars(
        f"file://{root_dir}/lib/flink-connector-jdbc-3.1.2-1.18.jar",
        f"file://{root_dir}/lib/postgresql-42.7.3.jar",
        f"file://{root_dir}/lib/flink-sql-connector-kafka-3.1.0-1.18.jar",
    )
    return env


def configure_source(server: str, earliest: bool = False) -> KafkaSource:
    """Makes kafka source initialization"""
    properties = {
        "bootstrap.servers": server,
        "group.id": "iot-sensors",
    }

    offset = KafkaOffsetsInitializer.latest()
    if earliest:
        offset = KafkaOffsetsInitializer.earliest()

    kafka_source = (
        KafkaSource.builder()
        .set_topics("sensors")
        .set_properties(properties)
        .set_starting_offsets(offset)
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )
    return kafka_source


def configure_postgre_sink(sql_dml: str, type_info: Types) -> JdbcSink:
    """Makes postgres sink initialization. Config params are set in this function."""
    return JdbcSink.sink(
        sql_dml,
        type_info,
        JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
        .with_url(f"jdbc:postgresql://{POSTGRES_HOST}/flinkdb")
        .with_driver_name("org.postgresql.Driver")
        .with_user_name("flinkuser")
        .with_password("flinkpassword")
        .build(),
        JdbcExecutionOptions.builder()
        .with_batch_interval_ms(1000)
        .with_batch_size(200)
        .with_max_retries(5)
        .build(),
    )


def configure_kafka_sink(server: str, topic_name: str) -> KafkaSink:

    return (
        KafkaSink.builder()
        .set_bootstrap_servers(server)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(topic_name)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .build()
    )


def main() -> None:
    """Main flow controller"""
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    logger.addHandler(logging.StreamHandler())

    # Initialize environment
    logger.info("Initializing environment")
    env = initialize_env()

    # Define source and sinks
    logger.info("Configuring source and sinks")
    kafka_source = configure_source(KAFKA_HOST)
    sql_dml_date = (
        "INSERT INTO date_data (full_data, weekday, weekmonth, day, month, year, quarter) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)"
    )
    sql_dml_time = (
        "INSERT INTO time_data (full_time, hour, minute, second, am_pm, shift) "
        "VALUES (?, ?, ?, ?, ?, ?)"
    )
    sql_dml_fact_content = (
        "INSERT INTO fact_content (id, sentiment_score, post_id, comment_id) "
        "VALUES (?, ?, ?, ?)"
    )
    sql_dml_social = (
        "INSERT INTO social_data (id, name, brand) "
        "VALUES (?, ?, ?)"
    )
    sql_dml_user = (
        "INSERT INTO user_data (user_id, name, handle, email, phone, address) "
        "VALUES (?, ?, ?, ?, ?, ?)"
    )
    sql_dml_post = (
        "INSERT INTO post_data (post_id, user_id, social_id, url, title, content) "
        "VALUES (?, ?, ?, ?, ?, ?)"
    )
    sql_dml_comment = (
        "INSERT INTO comment_data (comment_id, user_id, created_date, created_time, url, content, sentiment_score, count_react, count_like, count_dislike) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    )
    sql_dml_hashtag = (
        "INSERT INTO hashtag_data (id, hashtags) "
        "VALUES (?, ?)"
    )
    sql_dml_fact_posttag = (
        "INSERT INTO fact_posttag (id, post_id, hashtag_id) "
        "VALUES (?, ?, ?)"
    )
    
    TYPE_INFO_DATE = Types.ROW(
        [
            Types.STRING(),  # full_data
            Types.STRING(),  # weekday
            Types.STRING(),  # weekmonth
            Types.INT(),  # day
            Types.INT(),  # month
            Types.INT(),  # year
            Types.INT(),  # quarter
        ]
    )
    TYPE_INFO_TIME = Types.ROW(
        [
            Types.STRING(),  # full_time
            Types.INT(),  # hour
            Types.INT(),  # minute
            Types.INT(),  # second
            Types.STRING(),  # am_pm
            Types.STRING(),  # shift
        ]
    )
    TYPE_INFO_FACT_CONTENT = Types.ROW(
        [
            Types.STRING(),  # id
            Types.FLOAT(),  # sentiment_score
            Types.STRING(),  # post_id
            Types.STRING(),  # comment_id
        ]
    )
    TYPE_INFO_SOCIAL = Types.ROW(
        [
            Types.STRING(),  # id
            Types.STRING(),  # name
            Types.STRING(),  # brand
        ]
    )
    TYPE_INFO_USER = Types.ROW(
        [
            Types.STRING(),  # user_id
            Types.STRING(),  # name
            Types.STRING(),  # handle
            Types.STRING(),  # email
            Types.STRING(),  # phone
            Types.STRING(),  # address
        ]
    )
    TYPE_INFO_POST = Types.ROW(
        [
            Types.STRING(),  # post_id
            Types.STRING(),  # user_id
            Types.INT(),  # social_id
            Types.STRING(),  # url
            Types.STRING(),  # title
            Types.STRING(),  # content
        ]
    )
    TYPE_INFO_COMMENT = Types.ROW(
        [
            Types.STRING(),  # comment_id
            Types.STRING(),  # user_id
            Types.STRING(),  # created_date
            Types.STRING(),  # created_time
            Types.STRING(),  # url
            Types.STRING(),  # content
            Types.FLOAT(),  # sentiment_score
            Types.INT(),  # count_react
            Types.INT(),  # count_like
            Types.INT(),  # count_dislike
        ]
    )
    TYPE_INFO_HASHTAG = Types.ROW(
        [
            Types.STRING(),  # id
            Types.STRING(),  # hashtags
        ]
    )
    TYPE_INFO_FACT_POSTTAG = Types.ROW(
        [
            Types.STRING(),  # id
            Types.STRING(),  # post_id
            Types.STRING(),  # hashtag_id
        ]
    )
    jdbc_sink_date = configure_postgre_sink(sql_dml_date, TYPE_INFO_DATE)
    jdbc_sink_time = configure_postgre_sink(sql_dml_time, TYPE_INFO_TIME)
    jdbc_sink_fact_content = configure_postgre_sink(sql_dml_fact_content, TYPE_INFO_FACT_CONTENT)
    jdbc_sink_user = configure_postgre_sink(sql_dml_user, TYPE_INFO_USER)
    jdbc_sink_post = configure_postgre_sink(sql_dml_post, TYPE_INFO_POST)
    jdbc_sink_comment = configure_postgre_sink(sql_dml_comment, TYPE_INFO_COMMENT)
    jdbc_sink_hashtag = configure_postgre_sink(sql_dml_hashtag, TYPE_INFO_HASHTAG)
    jdbc_sink_fact_posttag = configure_postgre_sink(sql_dml_fact_posttag, TYPE_INFO_FACT_POSTTAG)
    logger.info("Source and sinks initialized")

    # Create a DataStream from the Kafka source and assign watermarks
    data_stream = env.from_source(
        kafka_source, WatermarkStrategy.no_watermarks(), "Kafka sensors topic"
    )

    # Make transformations to the data stream
    # transformed_data = data_stream.map(parse_data, output_type=TYPE_INFO)
    transformed_data_date = data_stream.map(parse_date_data, output_type=TYPE_INFO_DATE)
    transformed_data_time = data_stream.map(parse_time_data, output_type=TYPE_INFO_TIME)
    transformed_data_fact_content = data_stream.map(parse_fact_content_data, output_type=TYPE_INFO_FACT_CONTENT)
    transformed_data_user = data_stream.map(parse_user_data, output_type=TYPE_INFO_USER)
    transformed_data_post = data_stream.map(parse_post_data, output_type=TYPE_INFO_POST)
    transformed_data_comment = data_stream.flat_map(parse_comment_data, output_type=TYPE_INFO_COMMENT)
    transformed_data_hashtag = data_stream.map(parse_hashtag_data, output_type=TYPE_INFO_HASHTAG)
    transformed_data_fact_posttag = data_stream.map(parse_fact_posttag_data, output_type=TYPE_INFO_FACT_POSTTAG)
    logger.info("Defined transformations to data stream")

    logger.info("Ready to sink data")
    transformed_data_date.add_sink(jdbc_sink_date)
    transformed_data_time.add_sink(jdbc_sink_time)
    transformed_data_fact_content.add_sink(jdbc_sink_fact_content)
    transformed_data_user.add_sink(jdbc_sink_user)
    transformed_data_post.add_sink(jdbc_sink_post)
    transformed_data_comment.add_sink(jdbc_sink_comment)
    transformed_data_hashtag.add_sink(jdbc_sink_hashtag)
    transformed_data_fact_posttag.add_sink(jdbc_sink_fact_posttag)
    logger.info("Data sinked")

    # Execute the Flink job
    env.execute("Flink PostgreSQL and Kafka Sink")


if __name__ == "__main__":
    main()
