import json
import boto3
from kafka import KafkaProducer, KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic

BOOTSTRAP_SERVERS = []


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
    return {
        "GET": get,
        "PUT": put,
        "POST": post
    }[event["httpMethod"]](event)