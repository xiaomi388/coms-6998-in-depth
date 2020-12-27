import uuid
import boto3
import random
import time
import asyncio
import aioboto3
import http3

BASE_URL = "https://x1cg309g75.execute-api.us-east-1.amazonaws.com/v1/transaction"


class MockUser:
    @classmethod
    async def create(cls, id, loc):
        self = MockUser()
        self.id = id
        self.loc = loc

        # create the user in dynamodb
        async with aioboto3.resource("dynamodb", region_name="us-east-1") as dynamodb:
            table = await dynamodb.Table("user")
            await table.put_item(Item={
                "id": id,
                "last_location": loc
            })
        return self

    async def create_txn(self):
        """

        :return: 0 if txn is finished else 1
        """
        # mock the transaction information
        client = http3.AsyncClient(timeout=100)
        txn_id = str(uuid.uuid4())
        amount = random.randint(1, 130)
        item = ["iPhone12", "MacBook Pro", "Mac Mini", "iPad"][random.randint(0, 3)]
        location = self.loc if random.random() < 0.8 else "FakePlace"
        merchant_number = str(uuid.uuid4())
        # print("txn: ", txn_id)

        # grant the approval
        ret = await client.post(BASE_URL, json={
            "transaction_id": txn_id,
            "user_id": self.id,
            "amount": amount,
            "location": location
        })
        # print(ret)


        # poll to check if we've gotten the approval
        while (ret := await client.get(f"{BASE_URL}/{txn_id}")).status_code != 200:
            # print("waiting")
            await asyncio.sleep(1)
        txn = ret.json()["transaction"]
        if txn["state"] == "declined":
            return 1
        elif txn["state"] == "finished":
            return 0
        elif txn["state"] == "approved":
            # create the transaction
            ret = await client.put(f"{BASE_URL}/{txn_id}", json={
                "user_id": self.id,
                "item": item,
                "merchant_number": merchant_number,
                "location": location,
                "time": int(time.time()),
                "state": "finished"
            })
            # print(ret)

        # poll to check if the txn is finished
        while (ret := await client.get(f"{BASE_URL}/{txn_id}")).json()["transaction"]["state"] != "finished":
            print("waiting")
            await asyncio.sleep(1)
        return 0

async def test_concurrent():
    locations = ["New York City", "Beijing", "Mumbai", "Seoul"]
    mockusers = await asyncio.gather(*[MockUser.create(str(i), locations[random.randint(0, 3)]) for i in range(100)])

    start = time.time()
    await asyncio.wait([user.create_txn() for _ in range(1) for user in mockusers])
    end = time.time()
    # print(f"time of currently processing 100 transactions: {int(end-start)}(s)")

if __name__ == "__main__":
    asyncio.run(test_concurrent())




