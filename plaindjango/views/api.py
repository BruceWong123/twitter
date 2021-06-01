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
import sys


logger = logging.getLogger('django')

CONSUMER_KEY_1 = "4xIpesNGnicInoWrHz2eKKiGT"
CONSUMER_SECRET_1 = "895COTXg4ObAgSUfjbUDE0C5u1M1u5wuLy5mFjvJ6s4v1f35lM"
ACCESS_KEY_1 = "179379147-0ZzaSQF0Ek7mRpAiUi5K93saBSJtXt2n1CldQxeW"
ACCESS_SECRET_1 = "gZChNX5sSBtrjNpcFn3grumeDRJrIG7QLwmIH0n77eNzd"

CONSUMER_KEY_2 = "SiPtW0L6amxM8FRX8gbo4BBBg"
CONSUMER_SECRET_2 = "4Uuc8A0ud8x25DxbuKql20EyihQofo1Nlf4mVdxKFAaZOGfxLo"
ACCESS_KEY_2 = "1382248151352897538-sR1f4Vj5rIu5p0ZbdU7XIDCgK4YcUd"
ACCESS_SECRET_2 = "nOuCcvM9po0j1Lme21NlORN8lilsf6Czee6nnoPQvNIji"

auth = tweepy.OAuthHandler(CONSUMER_KEY_2, CONSUMER_SECRET_2)
auth.set_access_token(ACCESS_KEY_2, ACCESS_SECRET_2)

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


def get_followers(user_name):
    print("Set up Threading get followers of ", user_name)
    print('Number of current threads is ', threading.active_count())
    logger.info("into get followers >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
    db_users = mongo_db["users"]

    ids = []
    count = 0
    try:
        print("into get page")
        for page in tweepy.Cursor(tw_api.followers_ids, screen_name=user_name).pages():
            print("get page")
            ids.extend(page)
    except:
        print("Unexpected error:", sys.exc_info()[0])

    logger.info("done loading all followers")
    print("done loading all followers")

    final_ids = []
    for i, uid in enumerate(ids):
        user = tw_api.get_user(uid)
        if user.followers_count > 100:
            relation = tw_api.show_friendship(target_id=user.id)
            if relation[0].can_dm:
                logger.info(user.screen_name)

                key = {"id": user.id}
                data = {"screen_name": user.screen_name, "name": user.name, "id": user.id,
                        "follwers": user.followers_count, "location": user.location}
                db_users.update(key, data, upsert=True)

    logger.info("done inserting all into mongo")


@ api_view(['GET', 'PUT', 'DELETE'])
def get_followers_by_name(request, user_name):
    if request.method == 'GET':
        user_name = user_name.strip()
        logger.info("username is", user_name)
        # t = threading.Thread(target=get_followers,
        #                      args=(user_name,))
        # t.start()
        logger.info("get followers done")
        return HttpResponse("request sent")


@ api_view(['GET', 'PUT', 'DELETE'])
def send_direct_messages(request):
    if request.method == 'GET':
        logger.info("into send DM")

        # db_users = mongo_db["users"]

        # query_result = db_users.find()
        # for x in query_result:
        #     logger.info(x["name"].encode('utf-8'))
        logger.info("done DM")
        return HttpResponse("ok")


@ api_view(['GET', 'PUT', 'DELETE'])
def crm_manager(request):
    if request.method == 'GET':
        logger.info("into CRM")

        logger.info("done CRM")
        return HttpResponse("ok")
