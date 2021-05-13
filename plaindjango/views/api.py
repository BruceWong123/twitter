from rest_framework.response import Response
from rest_framework.decorators import api_view
from rest_framework import status
from django.http import HttpResponse, JsonResponse
from pymongo import MongoClient
import tweepy
import time
import json
import logging
import threading
from time import sleep, ctime


logger = logging.getLogger('django')

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

tw_api = tweepy.API(auth, wait_on_rate_limit=True,
                    wait_on_rate_limit_notify=True)

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


def get_followers(screen_name):
    print("into Threading get followers of ", screen_name)
    logger.info("into get followers >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
    db_users = mongo_db["users"]

    ids = []
    count = 0
    for page in tweepy.Cursor(tw_api.followers_ids, screen_name[0]).pages():
        ids.extend(page)

    logger.info("done loading all followers")

    final_ids = []
    for i, uid in enumerate(ids):
        user = tw_api.get_user(uid)
        if user.followers_count > 100:
            relation = tw_api.show_friendship(target_id=user.id)
            if relation[0].can_dm:
                logger.info(user.screen_name, user.name, user.id,
                            user.followers_count, user.location)

                key = {"id": user.id}
                data = {"screen_name": user.screen_name, "name": user.name, "id": user.id,
                        "follwers": user.followers_count, "location": user.location}
                db_users.update(key, data, upsert=True)

    logger.info("done inserting all into mongo")


@ api_view(['GET', 'PUT', 'DELETE'])
def followers(request, user_name):
    if request.method == 'GET':
        print("into get followers")
        t = threading.Thread(target=get_followers,
                             args=(user_name,))
        t.start()
        return HttpResponse("request sent")


@ api_view(['GET', 'PUT', 'DELETE'])
def test2(request):
    if request.method == 'GET':
        print("into test")
        time.sleep(120)
        logger.info("into tesst>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
        # print(str(request))
        db_users = mongo_db["users"]

        ids = []
        count = 0
        for page in tweepy.Cursor(tw_api.followers_ids, screen_name="AsensysChain").pages():
            ids.extend(page)

        logger.info("done loading all followers")

        final_ids = []
        for i, uid in enumerate(ids):
            user = tw_api.get_user(uid)
            if user.followers_count > 100:
                relation = tw_api.show_friendship(target_id=user.id)
                if relation[0].can_dm:
                    print(user.screen_name, user.name, user.id,
                          user.followers_count, user.location)

                    key = {"id": user.id}
                    data = {"screen_name": user.screen_name, "name": user.name, "id": user.id,
                            "follwers": user.followers_count, "location": user.location}
                    db_users.update(key, data, upsert=True)

        logger.info("done inserting all into mongo")

        return HttpResponse("ok")
