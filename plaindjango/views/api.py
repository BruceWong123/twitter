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

CONSUMER_KEY_3 = "SiPtW0L6amxM8FRX8gbo4BBBg"
CONSUMER_SECRET_3 = "4Uuc8A0ud8x25DxbuKql20EyihQofo1Nlf4mVdxKFAaZOGfxLo"
ACCESS_KEY_3 = "1382248151352897538-sR1f4Vj5rIu5p0ZbdU7XIDCgK4YcUd"
ACCESS_SECRET_3 = "nOuCcvM9po0j1Lme21NlORN8lilsf6Czee6nnoPQvNIji"

CONSUMER_KEY_2 = "sz6x0nvL0ls9wacR64MZu23z4"
CONSUMER_SECRET_2 = "ofeGnzduikcHX6iaQMqBCIJ666m6nXAQACIAXMJaFhmC6rjRmT"
ACCESS_KEY_2 = "854004678127910913-PUPfQYxIjpBWjXOgE25kys8kmDJdY0G"
ACCESS_SECRET_2 = "BC2TxbhKXkdkZ91DXofF7GX8p2JNfbpHqhshW1bwQkgxN"


def get_twitter_api(CONSUMER_KEY, CONSUMER_SECRET, ACCESS_KEY, ACCESS_SECRET):
    auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
    auth.set_access_token(ACCESS_KEY, ACCESS_SECRET)

    tw_api = tweepy.API(auth, wait_on_rate_limit=True,
                        wait_on_rate_limit_notify=True)
    return tw_api


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


def get_seed_users(key_word):
    print("get seed users")
    logger.info("get seed users on ", key_word)
    db_users = mongo_db["seedusers"]
    tw_api = get_twitter_api(
        CONSUMER_KEY_2, CONSUMER_SECRET_2, ACCESS_KEY_2, ACCESS_SECRET_2)
    try:
        count = 0
        for page in tweepy.Cursor(tw_api.search_users, key_word).pages():
            user = page[0]
            print(count, user.id, user.screen_name)
            count += 1
            key = {"id": user.id}
            data = {"screen_name": user.screen_name, "name": user.name, "id": user.id,
                    "follwers": user.followers_count, "location": user.location, "crawled": False}
            logger.info(data)
            db_users.update(key, data, upsert=True)
            logger.info("update done")
    except:
        print("Unexpected error:", sys.exc_info()[0])


def store_followers(ids):
    logger.info("start inserting all into mongo")
    tw_api = get_twitter_api(
        CONSUMER_KEY_3, CONSUMER_SECRET_3, ACCESS_KEY_3, ACCESS_SECRET_3)
    db_users = mongo_db["users"]
    for i, uid in enumerate(ids):
        user = tw_api.get_user(uid)
        if user.followers_count > 100:
            relation = tw_api.show_friendship(target_id=user.id)
            if relation[0].can_dm:
                logger.info(user.screen_name)
                key = {"id": user.id}
                data = {"screen_name": user.screen_name, "name": user.name, "id": user.id,
                        "follwers": user.followers_count, "location": user.location, "dmed": False}
                db_users.update(key, data, upsert=True)

    logger.info("done inserting all into mongo")


def get_followers(user_name):
    logger.info("Set up Threading get followers of ", user_name)
    logger.info('Number of current threads is ', threading.active_count())
    logger.info("into get followers >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
    tw_api = get_twitter_api(
        CONSUMER_KEY_3, CONSUMER_SECRET_3, ACCESS_KEY_3, ACCESS_SECRET_3)

    count = 0
    try:
        logger.info("into get page")
        for page in tweepy.Cursor(tw_api.followers_ids, screen_name=user_name).pages():
            ids = []
            logger.info("get page")
            ids.extend(page)
            logger.info(len(ids))
            store_followers(ids)
    except:
        print("Unexpected error:", sys.exc_info()[0])

    logger.info("done loading all followers")
    print("done loading all followers")


def send_direct_message(list_of_users, text):
    logger.info("into direct message")
    logger.info(list_of_users)
    tw_api = get_twitter_api(
        CONSUMER_KEY_1, CONSUMER_SECRET_1, ACCESS_KEY_1, ACCESS_SECRET_1)

    try:

        for user in list_of_users:
            logger.info("send dm to ", user['name'])
            message = "Hi " + user["name"] + "\n\n" + text
            direct_message = tw_api.send_direct_message(user["id"], message)
            print(user)

    except:
        print("Unexpected error:", sys.exc_info()[0])


@ api_view(['GET', 'PUT', 'DELETE'])
def get_followers_by_name(request, user_name):
    if request.method == 'GET':
        user_name = user_name.strip()
        logger.info("get followers of ", user_name)
        t = threading.Thread(target=get_followers,
                             args=(user_name,))
        t.start()
        logger.info("get followers done")
        return HttpResponse("request sent")


@ api_view(['GET', 'PUT', 'DELETE'])
def get_seed_users_by_key(request, key_word):
    if request.method == 'GET':
        logger.info("keyword is", key_word)
        print("keyword is", key_word)
        t = threading.Thread(target=get_seed_users,
                             args=(key_word,))
        t.start()
        logger.info("get seed user done")
        return HttpResponse("request sent")


@ api_view(['GET', 'PUT', 'DELETE'])
def send_direct_messages(request):
    if request.method == 'PUT':
        logger.info("into send DM 111111")
        request_body = request.data
        logger.info(request_body)
        user_list = request_body["users"]

        content = request_body["content"]
        logger.info(user_list)
        logger.info(content)
        t = threading.Thread(target=send_direct_message,
                             args=(user_list, content))
        t.start()
        logger.info("done DM")
        return HttpResponse("ok")


@ api_view(['GET', 'PUT', 'DELETE'])
def crm_manager(request):
    if request.method == 'GET':
        logger.info("into CRM")

        logger.info("done CRM")
        return HttpResponse("ok")
