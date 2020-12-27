import json
import boto3
import collections
import time


def lambda_handler(event, context):
    dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
    table = dynamodb.Table("transaction")
    scan_kwargs = {}
    done = False
    start_key = None
    users = collections.defaultdict(list)
    while not done:
        if start_key:
            scan_kwargs['ExclusiveStartKey'] = start_key
        response = table.scan(**scan_kwargs)
        items = response.get('Items', [])
        cur_time = int(time.time())
        for item in items:
            if item["state"] != "finished": continue
            # only choose transactions within the last month
            if int(item["time"]) >= cur_time - 60 * 60 * 24 * 30:
                users[item["user_id"]].append(item)
        start_key = response.get('LastEvaluatedKey', None)
        done = start_key is None

    # TODO: instead of simply printting the transactions,
    # we can send the billing statement to all users by SMS
    print(users)
    return {
        'statusCode': 200,
    }
