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
import mysql.connector as mysql
import random
logger = logging.getLogger('django')


level3_keys = []
level2_keys = []
level1_keys = []


HOST = "170.106.188.105"  # or "domain.com"
# database name, if you want just to connect to MySQL server, leave it empty
DATABASE = "task_data"
# this is the user you create
USER = "root"
# user password
PASSWORD = "abc12341"
# connect to MySQL server


def load_level3_keys():
    mysql_connection = mysql.connect(
        host=HOST, database=DATABASE, user=USER, password=PASSWORD, buffered=True)
    print("Connected to:", mysql_connection.get_server_info())
    mysql_cursor = mysql_connection.cursor(buffered=True)

    sql = "SELECT * FROM asynctask_api_key WHERE status = 'level3'"
    mysql_cursor.execute(sql)

    query_result = mysql_cursor.fetchall()

    for row in query_result:
        key = {}
        key["CONSUMER_KEY"] = row[0]
        key["CONSUMER_SECRET"] = row[1]
        key["ACCESS_KEY"] = row[2]
        key["ACCESS_SECRET"] = row[3]
        level3_keys.append(key)
    print("load level3 keys done ", len(level3_keys))

    mysql_cursor.close()
    mysql_connection.close()


def load_level2_keys():
    mysql_connection = mysql.connect(
        host=HOST, database=DATABASE, user=USER, password=PASSWORD, buffered=True)
    print("Connected to:", mysql_connection.get_server_info())
    mysql_cursor = mysql_connection.cursor(buffered=True)

    sql = "SELECT * FROM asynctask_api_key WHERE status = 'level2'"
    mysql_cursor.execute(sql)

    query_result = mysql_cursor.fetchall()

    for row in query_result:
        key = {}
        key["CONSUMER_KEY"] = row[0]
        key["CONSUMER_SECRET"] = row[1]
        key["ACCESS_KEY"] = row[2]
        key["ACCESS_SECRET"] = row[3]
        level2_keys.append(key)
    print("load level2 keys done ", len(level2_keys))
    mysql_cursor.close()
    mysql_connection.close()


def load_level1_keys():
    mysql_connection = mysql.connect(
        host=HOST, database=DATABASE, user=USER, password=PASSWORD, buffered=True)
    print("Connected to:", mysql_connection.get_server_info())
    mysql_cursor = mysql_connection.cursor(buffered=True)

    sql = "SELECT * FROM asynctask_api_key WHERE status = 'level1'"
    mysql_cursor.execute(sql)

    query_result = mysql_cursor.fetchall()

    for row in query_result:
        key = {}
        key["CONSUMER_KEY"] = row[0]
        key["CONSUMER_SECRET"] = row[1]
        key["ACCESS_KEY"] = row[2]
        key["ACCESS_SECRET"] = row[3]
        level1_keys.append(key)
    print("load level1 keys done ", len(level1_keys))
    mysql_cursor.close()
    mysql_connection.close()


load_level1_keys()
load_level2_keys()
load_level3_keys()


def get_twitter_api(level):
    print("level %d " % level)
    key_list = []
    if level == 1:
        key_list = level1_keys
    elif level == 2:
        key_list = level2_keys
    elif level == 3:
        key_list = level3_keys
    print("stop %d" % (len(key_list)-1))
    idx = random.randint(0, len(key_list)-1)
    print("get random index %d", idx)
    key = key_list[idx]
    auth = tweepy.OAuthHandler(key["CONSUMER_KEY"], key["CONSUMER_SECRET"])
    auth.set_access_token(key["ACCESS_KEY"], key["ACCESS_SECRET"])

    logger.info(key)

    tw_api = tweepy.API(auth, wait_on_rate_limit=True,
                        wait_on_rate_limit_notify=True)
    return tw_api


get_twitter_api(1)
get_twitter_api(2)
get_twitter_api(3)

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


def send_error_message(tw_api, error_message):
    direct_message = tw_api.send_direct_message("bruce_ywong", error_message)


def get_seed_users(key_word):
    print("get seed users")
    logger.info("get seed users on %s " % key_word)
    db_users = mongo_db["seedusers"]
    tw_api = get_twitter_api(3)
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
            time.sleep(1000)
    except tweepy.TweepError as e:
        print("Tweepy Error: {}".format(e))
        logger.info("Tweepy Error: {}".format(e))
        send_error_message(tw_api, format(e))


def store_followers(ids):
    logger.info("start inserting all into mongo")
    tw_api = get_twitter_api(2)
    db_users = mongo_db["users"]
    count = 0
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
                count += 1
                time.sleep(120)
    logger.info("done inserting all into mongo")


def get_followers(user_name):
    print("Set up Threading get followers of %s" % user_name)
    logger.info("Set up Threading get followers of %s" % user_name)
    logger.info('Number of current threads is %d', threading.active_count())
    logger.info("into get followers >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
    tw_api = get_twitter_api(2)

    count = 0
    try:
        logger.info("into get page")
        for page in tweepy.Cursor(tw_api.followers_ids, screen_name=user_name).pages():
            ids = []
            ids.extend(page)
            logger.info("get new page with ids of %d" % len(ids))
            print("get new page with ids of %d" % len(ids))
            store_followers(ids)
            time.sleep(3000)
    except tweepy.TweepError as e:
        print("Tweepy Error: {}".format(e))
        logger.info("Tweepy Error: {}".format(e))
        send_error_message(tw_api, format(e))
    logger.info("done loading all followers")
    print("done loading all followers")


def send_direct_message(list_of_users, text):
    logger.info("into direct message")
    logger.info(list_of_users)
    tw_api = get_twitter_api(1)

    try:
        for user in list_of_users:
            logger.info("send dm to %s " % user['name'])
            firstname = user["name"].split(' ')[0]
            message = "Hi " + firstname + ",\n\n" + text
            direct_message = tw_api.send_direct_message(user["id"], message)
#           logger.info("direct message id: " + direct_message.id)
#           tw_api.destroy_direct_message(direct_message.id)
    except tweepy.TweepError as e:
        print("Tweepy Error: {}".format(e))
        logger.info("Tweepy Error: {}".format(e))
        send_error_message(tw_api, format(e))


@ api_view(['GET', 'PUT', 'DELETE'])
def get_followers_by_name(request, user_name):
    if request.method == 'GET':
        user_name = user_name.strip()
        logger.info("create thread get followers of %s " % user_name)
        t = threading.Thread(target=get_followers,
                             args=(user_name,))
        t.start()
        return HttpResponse("request sent")


@ api_view(['GET', 'PUT', 'DELETE'])
def get_seed_users_by_key(request, key_word):
    if request.method == 'GET':
        logger.info("keyword is %s " % key_word)
        print("keyword is %s " % key_word)
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


def store_direct_message(direct_message):
    mysql_connection = mysql.connect(
        host=HOST, database=DATABASE, user=USER, password=PASSWORD, buffered=True)
    print("Connected to:", mysql_connection.get_server_info())
    mysql_cursor = mysql_connection.cursor(buffered=True)

    sql = "INSERT ignore INTO asynctask_message (messageid, sender, receiver, type, content, replied, time) VALUES (%s, %s,%s,%s,%s,%s,%s)"
    val = (direct_message.id, direct_message.message_create['sender_id'],
           direct_message.message_create['target']['recipient_id'], direct_message.type,
           str(direct_message.message_create['message_data']['text']), "no", direct_message.created_timestamp)

    mysql_cursor.execute(sql, val)

    mysql_connection.commit()

    mysql_cursor.close()
    mysql_connection.close()


@ api_view(['GET', 'PUT', 'DELETE'])
def crm_manager(request):
    if request.method == 'GET':
        logger.info("into CRM")
    load_level1_keys()
    logger.info("level1 keys: %d" % len(level1_keys))
    for key in level1_keys:
        logger.info(key)
        db_users = mongo_db["messages"]
        auth = tweepy.OAuthHandler(
            key["CONSUMER_KEY"], key["CONSUMER_SECRET"])
        auth.set_access_token(key["ACCESS_KEY"], key["ACCESS_SECRET"])

        tw_api = tweepy.API(auth, wait_on_rate_limit=True,
                            wait_on_rate_limit_notify=True)

        direct_messages = tw_api.list_direct_messages()

        logger.info("the number of messages is %d " % len(direct_messages))
        for direct_message in direct_messages:
            if direct_message.message_create['target']['recipient_id'] == '179379147':
                logger.info(direct_message.created_timestamp)
                logger.info("The type is : " + direct_message.type)
                logger.info("The id is : " + direct_message.id)
                logger.info("The recipient_id is : " +
                            direct_message.message_create['target']['recipient_id'])
                logger.info("The sender_id is : " +
                            direct_message.message_create['sender_id'])
                logger.info(
                    "The text is : " + str(direct_message.message_create['message_data']['text']))
                store_direct_message(direct_message)
    logger.info("done CRM")
    return HttpResponse("ok")


@ api_view(['GET', 'PUT', 'DELETE'])
def refresh_api(request):
    if request.method == 'GET':
        logger.info("start refresh api")
        load_level1_keys()
        load_level2_keys()
        load_level3_keys()
        logger.info("done refresh api")
        return HttpResponse("ok")
