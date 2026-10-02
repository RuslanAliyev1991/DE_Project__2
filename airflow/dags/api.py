import requests
import datetime, time
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
import json

USER = "ruslan"
PASSWORD = "Ruslan1991"
URL = "https://opensky-network.org/api/states/all"

TOKEN_URL = "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token"
CLIENT_ID="ruslan-api-client"
CLIENT_SECRET="CMa4t36A4rpKAMZUbNeI5yFPwllpQlB3"

PARAMS = {
    "lamin": 45.8389,
    "lomin": 5.9962,
    "lamax": 47.8229,
    "lomax": 10.5226

    
}

# How many seconds before expiry to proactively refresh the token.
TOKEN_REFRESH_MARGIN = 30

CONFIG = {
    'bootstrap.servers': 'kafka-1:9092',
    'bootstrap.servers': 'kafka-2:9092'
}


class myApi:
    def __init__(self):
        self.token = None
        self.expires_at = None

    def get_token(self):
        """Return a valid access token, refreshing automatically if needed."""
        if self.token and self.expires_at and datetime.datetime.now() < self.expires_at:
            return self.token
        return self._refresh()

    def _refresh(self):
        """Fetch a new access token from the OpenSky authentication server."""
        r = requests.post(
            TOKEN_URL,
            data={
                "grant_type": "client_credentials",
                "client_id": CLIENT_ID,
                "client_secret": CLIENT_SECRET,
            },
        )
        r.raise_for_status()

        data = r.json()
        self.token = data["access_token"]
        expires_in = data.get("expires_in", 1800)
        self.expires_at = datetime.datetime.now() + datetime.timedelta(seconds=expires_in - TOKEN_REFRESH_MARGIN)
        return self.token

    def headers(self):
        """Return request headers with a valid Bearer token."""
        return {"Authorization": f"Bearer {self.get_token()}"}
    

    def create_topic_if_not_exists(self, topic_name):
        admin_client = AdminClient(CONFIG)
        metadata = admin_client.list_topics(timeout=5)
        if topic_name not in metadata.topics:
            print(f"'{topic_name}' tapilmadi. Yaradilir...")
            new_topic = NewTopic(topic_name, num_partitions=3, replication_factor=2)
            fs = admin_client.create_topics([new_topic])
            for topic, f in fs.items():
                try:
                    f.result()
                    print(f"Topic '{topic}' ugurla yaradildi. f-in deyeri: {f}")
                except Exception as e:
                    print(f"Yaradilma xetasi: {e}")
            return topic_name
        else:
            print(f"Topic '{topic_name}' artiq movcuddur.")
            return topic_name


# Create a single shared instance for your script.
my_data = myApi()
producer = Producer(CONFIG)

# Use it for any API call - the token is refreshed automatically.
while True:
    response = requests.get(
        URL,
        headers=my_data.headers(),
        params=PARAMS
    )

    producer.produce(
        topic=my_data.create_topic_if_not_exists("mytopic"),
        value=response.content
        #callback=delivery_report
    )       
    producer.flush()
    print(response.content)
    #print(response.json())
    print("=======================================================================================")
    time.sleep(4)