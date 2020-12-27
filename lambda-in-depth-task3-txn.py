import json
import boto3
import base64

def lambda_handler(event, context):
    for partition in event["records"]:
        for record in event["records"][partition]:
            txn_req = json.loads(base64.b64decode(record["value"]).decode())
            dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
            table = dynamodb.Table("transaction")
            item = table.get_item(Key={"id": txn_req["id"]}).get("Item", None)
            if item:
                return {"statusCode": 200}
            table.put_item(Item=txn_req)
    return {
        "statusCode": 200
    }
