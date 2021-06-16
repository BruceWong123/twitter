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
from datetime import datetime
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


def insert_stat_info(user, dm, reply):
    logger.info("record stat %s %s %s" % (user, dm, reply))
    if user == 0 and dm == 0 and reply == 0:
        return
    mysql_connection = mysql.connect(
        host=HOST, database=DATABASE, user=USER, password=PASSWORD, buffered=True)
    print("Connected to:", mysql_connection.get_server_info())
    mysql_cursor = mysql_connection.cursor(buffered=True)

    sql = "INSERT INTO asynctask_stat (new_user, dm, reply, date) VALUES (%s, %s,%s,%s)"
    val = (user, dm, reply, datetime.now())
    logger.info(val)
    mysql_cursor.execute(sql, val)
    mysql_connection.commit()

    logger.info("insert stat done")

    mysql_cursor.close()
    mysql_connection.close()


def load_level3_keys():
    level3_keys.clear()
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
        key["ID"] = row[7]
        level3_keys.append(key)
    print("load level3 keys done ", len(level3_keys))

    mysql_cursor.close()
    mysql_connection.close()


def load_level2_keys():
    level2_keys.clear()
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
        key["ID"] = row[7]
        level2_keys.append(key)
    print("load level2 keys done ", len(level2_keys))
    mysql_cursor.close()
    mysql_connection.close()


def load_level1_keys():
    level1_keys.clear()
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
        key["ID"] = row[7]
        level1_keys.append(key)
    print("load level1 keys done ", len(level1_keys))
    mysql_cursor.close()
    mysql_connection.close()


load_level1_keys()
load_level2_keys()
load_level3_keys()


def get_twitter_api_by_id(api_id):
    for key in level1_keys:
        if key["ID"] == api_id:
            auth = tweepy.OAuthHandler(
                key["CONSUMER_KEY"], key["CONSUMER_SECRET"])
            auth.set_access_token(key["ACCESS_KEY"], key["ACCESS_SECRET"])

            logger.info(key)

            tw_api = tweepy.API(auth, wait_on_rate_limit=True,
                                wait_on_rate_limit_notify=True)
            return tw_api
    logger.info("could not find api by id !!!!!!!")
    return None


def get_twitter_api(level):
    logger.info("level %d " % level)
    key_list = []
    if level == 1:
        print("111")
        key_list = level1_keys
        print("level1 keys len ", len(level1_keys))
    elif level == 2:
        print("222")
        key_list = level2_keys
    elif level == 3:
        print("333")
        key_list = level3_keys
    logger.info("stop %d" % (len(key_list)-1))
    idx = 0
    if len(key_list) > 1:
        idx = random.randint(0, len(key_list)-1)
    logger.info("get random index %d", idx)
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
    insert_stat_info(count, 0, 0)
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


def send_direct_message(list_of_users, text, tw_api, is_reply):
    logger.info("into direct message")
    logger.info("Set up Threading send direct message")
    logger.info('Number of current threads is %d', threading.active_count())
    logger.info("send direct message >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
    logger.info(list_of_users)

    try:
        for user in list_of_users:
            logger.info("in circle")
            logger.info("send dm to %s " % user['name'])
            firstname = user["name"].split(' ')[0]

            message = "Hi " + firstname + ",\n\n" + text
            if is_reply:
                logger.info("it is a reply")
                message = text
            direct_message = tw_api.send_direct_message(user["id"], message)
#           logger.info("direct message id: " + direct_message.id)
#           tw_api.destroy_direct_message(direct_message.id)
        insert_stat_info(0, len(list_of_users), 0)
    except tweepy.TweepError as e:
        print("Tweepy Error: {}".format(e))
        logger.info("Tweepy Error: {}".format(e))
        send_error_message(tw_api, format(e))
    logger.info("done threading send DM")


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
        tw_api = get_twitter_api(1)
        is_reply = False
        if 'api_id' in request_body.keys():
            logger.info("found api id in request")
            tw_api = get_twitter_api_by_id(request_body["api_id"])
            is_reply = True
        logger.info(user_list)
        logger.info(content)
        t = threading.Thread(target=send_direct_message,
                             args=(user_list, content, tw_api, is_reply))
        t.start()
        logger.info("done DM")
        return HttpResponse("ok")


def store_direct_message(direct_message, sender_name, receiver_name):
    mysql_connection = mysql.connect(
        host=HOST, database=DATABASE, user=USER, password=PASSWORD, buffered=True)
    print("Connected to:", mysql_connection.get_server_info())
    mysql_cursor = mysql_connection.cursor(buffered=True)

    date = datetime.fromtimestamp(int(direct_message.created_timestamp)/1000)
    date_object = date.date().isoformat()
    sql = "INSERT ignore INTO asynctask_message (messageid, sender, receiver, type, content, replied, date,sender_name, receiver_name) VALUES (%s, %s,%s,%s,%s,%s,%s,%s,%s)"
    val = (direct_message.id, direct_message.message_create['sender_id'],
           direct_message.message_create['target']['recipient_id'], direct_message.type,
           str(direct_message.message_create['message_data']['text']), "no", date_object, sender_name, receiver_name)

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
        count = 0
        for direct_message in direct_messages:
            if direct_message.message_create['target']['recipient_id'] == key["ID"]:
                logger.info(direct_message.created_timestamp)
                logger.info("The type is : " + direct_message.type)
                logger.info("The id is : " + direct_message.id)
                logger.info("The recipient_id is : " +
                            direct_message.message_create['target']['recipient_id'])
                logger.info("The sender_id is : " +
                            direct_message.message_create['sender_id'])
                logger.info(
                    "The text is : " + str(direct_message.message_create['message_data']['text']))
                sender_name = tw_api.get_user(
                    direct_message.message_create['sender_id']).screen_name
                receiver_name = tw_api.get_user(
                    direct_message.message_create['target']['recipient_id']).screen_name
                store_direct_message(
                    direct_message, sender_name, receiver_name)
                count += 1
        insert_stat_info(0, 0, count)
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
