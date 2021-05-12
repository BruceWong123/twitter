from rest_framework.response import Response
from rest_framework.decorators import api_view
from rest_framework import status
from django.http import HttpResponse, JsonResponse
from pymongo import MongoClient
import tweepy
import time
import json

# bruce
# CONSUMER_KEY = "4xIpesNGnicInoWrHz2eKKiGT"
# CONSUMER_SECRET = "895COTXg4ObAgSUfjbUDE0C5u1M1u5wuLy5mFjvJ6s4v1f35lM"
# ACCESS_KEY = "179379147-0ZzaSQF0Ek7mRpAiUi5K93saBSJtXt2n1CldQxeW"
# ACCESS_SECRET = "gZChNX5sSBtrjNpcFn3grumeDRJrIG7QLwmIH0n77eNzd"

CONSUMER_KEY = "SiPtW0L6amxM8FRX8gbo4BBBg"
CONSUMER_SECRET = "4Uuc8A0ud8x25DxbuKql20EyihQofo1Nlf4mVdxKFAaZOGfxLo"
ACCESS_KEY = "1382248151352897538-sR1f4Vj5rIu5p0ZbdU7XIDCgK4YcUd"
ACCESS_SECRET = "nOuCcvM9po0j1Lme21NlORN8lilsf6Czee6nnoPQvNIji"

auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
auth.set_access_token(ACCESS_KEY, ACCESS_SECRET)

tw_api = tweepy.API(auth, wait_on_rate_limit=True)

host = '43.130.32.126'
port = 27017
authSource = "tw"
my_mongo_client = MongoClient(
    "mongodb://%s:%s@%s:%s/%s" % ('bruce',
                                  'twitter', host, port, 'tw')
)
mongo_db = my_mongo_client["tw"]
print(
    "Successfully connected to Mongo"
)


@ api_view(['GET', 'PUT', 'DELETE'])
def test(request):
    if request.method == 'GET':
        print("into test")
        db_users = mongo_db["users"]

        ids = []
        count = 0
        for page in tweepy.Cursor(tw_api.followers_ids, screen_name="AsensysChain").pages():
            ids.extend(page)
            break

        final_ids = []
        for i, uid in enumerate(ids):
            user = tw_api.get_user(uid)
            relation = tw_api.show_friendship(target_id=user.id)
            if user.followers_count > 100 and relation[0].can_dm:
                print(user.screen_name, user.name, user.id,
                      user.followers_count, user.location)

                key = {"id": user.id}
                data = {"screen_name": user.screen_name, "name": user.name, "id": user.id,
                        "follwers": user.followers_count, "location": user.location}
                db_users.update(key, data, upsert=True)

                # db_users.insert_one(
                #     {"screen_name": user.screen_name, "name": user.name, "id": user.id, "follwers": user.followers_count, "location": user.location})
                # time.sleep(10)
                # final_ids.append(user.screen_name)
        # print(final_ids)

        return HttpResponse("ok")
