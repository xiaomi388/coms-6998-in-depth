import json
import boto3
import base64
import time
from kafka import KafkaProducer, KafkaConsumer

BOOTSTRAP_SERVERS = ["xxxx"]


def check(request):
    dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
    table = dynamodb.Table("user")
    user_id = request["user_id"]
    item = table.get_item(Key={"id": user_id}).get("Item", None)
    if not item:
        return False
    # do some basic checks
    if "last_location" in item and item["last_location"] != request["location"]:
        return False
    if request["amount"] > 100:
        return False
    return True


def lambda_handler(event, context):
    print(event)
    producer = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVERS)
    for partition in event["records"]:
        for record in event["records"][partition]:
            request = json.loads(base64.b64decode(record["value"]).decode())
            data = {
                "id": request["transaction_id"],
                "user_id": request["user_id"],
                "location": request["location"],
                "amount": request["amount"],
                "time": int(time.time()),
                "state": "approved" if check(request) else "declined"
            }
            producer.send("transaction-initialization", json.dumps(data).encode())
            print(data)
    producer.flush()
    return {
        "statusCode": 200,
        "body": "xxx"
    }
