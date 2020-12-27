import json
import boto3
from kafka import KafkaProducer, KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic

BOOTSTRAP_SERVERS = ["b-1.in-depth-task3.35ph0c.c9.kafka.us-east-1.amazonaws.com:9092",
                     "b-2.in-depth-task3.35ph0c.c9.kafka.us-east-1.amazonaws.com:9092"]


def get(event):
    dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
    table = dynamodb.Table("transaction")
    txn_id = event["pathParameters"]["id"]
    item = table.get_item(Key={"id": txn_id}).get("Item", None)
    if not item:
        return {
            "statusCode": 404,
        }
    return {
        "statusCode": 200,
        "body": json.dumps({"transaction": {
            "id": item["id"],
            "state": item["state"],
            "time": int(item["time"])
        }})
    }


def post(event):
    # admin_client = KafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS, client_id='test')
    # topic_list = []
    # topic_list.append(NewTopic(name="transaction", num_partitions=1, replication_factor=2))
    # admin_client.create_topics(new_topics=topic_list, validate_only=False)
    producer = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVERS)
    producer.send('approval', event["body"].encode())
    producer.flush()
    return {
        "statusCode": 201
    }


def put(event):
    producer = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVERS)
    data = json.loads(event["body"])
    data["id"] = event["pathParameters"]["id"]
    producer.send('transaction', key=data["id"].encode(), value=json.dumps(data).encode())
    producer.flush()
    print(data)
    return {
        "statusCode": 202,
        "body": json.dumps(data)
    }


def lambda_handler(event, context):
    # return {
    #     "statusCode": 200,
    #     "body": json.dumps(event)
    # }
    return {
        "GET": get,
        "PUT": put,
        "POST": post
    }[event["httpMethod"]](event)