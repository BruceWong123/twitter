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
import os
from requests import get

logger = logging.getLogger('django')


level3_keys = []
level2_keys = []
level1_keys = []

level1_index = 0
level2_index = 0
level3_index = 0

dm_contents = []

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

    sql = "INSERT INTO asynctask_stat (new_user, dm, reply, date, new_account) VALUES (%s, %s,%s,%s,%s)"
    val = (user, dm, reply, datetime.now(), 0)
    logger.info(val)
    mysql_cursor.execute(sql, val)
    mysql_connection.commit()

    logger.info("insert stat done")

    mysql_cursor.close()
    mysql_connection.close()


def server_account(account):
    mysql_connection = mysql.connect(
        host=HOST, database=DATABASE, user=USER, password=PASSWORD, buffered=True)
    print("Connected to:", mysql_connection.get_server_info())
    mysql_cursor = mysql_connection.cursor(buffered=True)

    ip = get('https://api.ipify.org').text
    logger.info('My public IP address is: {}'.format(ip))

    sql = "Update asynctask_server Set accounts = \"" + \
        str(account) + "\" Where ip = " + '\'' + str(ip) + '\''

    mysql_cursor.execute(sql)

    mysql_connection.commit()
    mysql_cursor.close()
    mysql_connection.close()


def server_request():
    mysql_connection = mysql.connect(
        host=HOST, database=DATABASE, user=USER, password=PASSWORD, buffered=True)
    print("Connected to:", mysql_connection.get_server_info())
    mysql_cursor = mysql_connection.cursor(buffered=True)

    ip = get('https://api.ipify.org').text
    logger.info('My public IP address is: {}'.format(ip))
    sql = "SELECT * FROM asynctask_server WHERE ip = " + '\'' + str(ip) + '\''
    logger.info(sql)
    mysql_cursor.execute(sql)
    request_count = 0
    query_result = mysql_cursor.fetchall()
    for row in query_result:
        request_count = int(row[6])
    request_count = request_count + 1

    sql = "Update asynctask_server Set requests = \"" + \
        str(request_count) + "\" Where ip = " + '\'' + str(ip) + '\''

    mysql_cursor.execute(sql)

    mysql_connection.commit()
    mysql_cursor.close()
    mysql_connection.close()


def load_level3_keys():
    level3_keys.clear()
    mysql_connection = mysql.connect(
        host=HOST, database=DATABASE, user=USER, password=PASSWORD, buffered=True)
    print("Connected to:", mysql_connection.get_server_info())
    mysql_cursor = mysql_connection.cursor(buffered=True)

    sql = "SELECT * FROM asynctask_api_key WHERE level = '3'"
    mysql_cursor.execute(sql)

    query_result = mysql_cursor.fetchall()

    for row in query_result:
        key = {}
        key["CONSUMER_KEY"] = row[0]
        key["CONSUMER_SECRET"] = row[1]
        key["ACCESS_KEY"] = row[2]
        key["ACCESS_SECRET"] = row[3]
        key["ID"] = row[7]
        key["LAST"] = row[10]
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

    ip = get('https://api.ipify.org').text
    logger.info('My public IP address is: {}'.format(ip))
    sql = "SELECT * FROM asynctask_server WHERE ip = " + '\'' + str(ip) + '\''
    logger.info(sql)
    mysql_cursor.execute(sql)
    server_id = -1
    query_result = mysql_cursor.fetchall()
    for row in query_result:
        server_id = row[3]
        logger.info("server id %s " % server_id)

    sql = "SELECT * FROM asynctask_api_key WHERE level = '2' and server_id = " + \
        '\'' + str(server_id) + '\''
    logger.info(sql)
    mysql_cursor.execute(sql)

    query_result = mysql_cursor.fetchall()

    for row in query_result:
        key = {}
        key["CONSUMER_KEY"] = row[0]
        key["CONSUMER_SECRET"] = row[1]
        key["ACCESS_KEY"] = row[2]
        key["ACCESS_SECRET"] = row[3]
        key["ID"] = row[7]
        key["LAST"] = row[10]
        key["SERVER_ID"] = str(server_id)

        level2_keys.append(key)
    logger.info("load level2 keys done %d " % len(level2_keys))
    print("load level2 keys done ", len(level2_keys))
    mysql_cursor.close()
    mysql_connection.close()


def load_dmcontents():
    mysql_connection = mysql.connect(
        host=HOST, database=DATABASE, user=USER, password=PASSWORD, buffered=True)
    print("Connected to:", mysql_connection.get_server_info())
    mysql_cursor = mysql_connection.cursor(buffered=True)

    sql = "SELECT * FROM asynctask_campaign_content"
    mysql_cursor.execute(sql)

    query_result = mysql_cursor.fetchall()

    for row in query_result:
        dm_contents.append((row[0], row[2]))
    mysql_cursor.close()
    mysql_connection.close()


def insert_last_reply(user_id, last_timestamp):

    mysql_connection = mysql.connect(
        host=HOST, database=DATABASE, user=USER, password=PASSWORD, buffered=True)
    print("Connected to:", mysql_connection.get_server_info())
    mysql_cursor = mysql_connection.cursor(buffered=True)

    sql = "Update asynctask_api_key Set last_reply = \"" + str(last_timestamp) + "\" Where user_id = " + \
        "\"" + str(user_id) + "\""
    mysql_cursor.execute(sql)

    mysql_connection.commit()
    mysql_cursor.close()
    mysql_connection.close()


def load_level1_keys():
    logger.info("loading level 1 key")
    level1_keys.clear()
    mysql_connection = mysql.connect(
        host=HOST, database=DATABASE, user=USER, password=PASSWORD, buffered=True)
    print("Connected to:", mysql_connection.get_server_info())
    mysql_cursor = mysql_connection.cursor(buffered=True)
    ip = get('https://api.ipify.org').text
    logger.info('My public IP address is: {}'.format(ip))

    sql = "SELECT * FROM asynctask_server WHERE ip = " + '\'' + str(ip) + '\''
    logger.info(sql)
    print(sql)

    logger.info(sql)
    mysql_cursor.execute(sql)
    server_id = -1
    query_result = mysql_cursor.fetchall()
    for row in query_result:
        logger.info("query result")
        server_id = row[3]
        logger.info("server id %s " % server_id)

    sql = "SELECT * FROM asynctask_api_key WHERE level = '1' and server_id = " + \
        '\'' + str(server_id) + '\''
    logger.info(sql)
    print(sql)
    mysql_cursor.execute(sql)
    query_result = mysql_cursor.fetchall()

    for row in query_result:
        key = {}
        key["CONSUMER_KEY"] = row[0]
        key["CONSUMER_SECRET"] = row[1]
        key["ACCESS_KEY"] = row[2]
        key["ACCESS_SECRET"] = row[3]
        key["ID"] = row[7]
        key["LAST"] = row[10]  # date for last crawling of reply message
        key["SERVER_ID"] = str(server_id)

        level1_keys.append(key)
    logger.info("load level1 keys done %d " % len(level1_keys))
    print("load level1 keys done %d " % len(level1_keys))
    mysql_cursor.close()
    mysql_connection.close()

    server_account(len(level1_keys))


def refresh_followers():
    mysql_connection = mysql.connect(
        host=HOST, database=DATABASE, user=USER, password=PASSWORD, buffered=True)
    print("Connected to:", mysql_connection.get_server_info())
    mysql_cursor = mysql_connection.cursor(buffered=True)

    ip = get('https://api.ipify.org').text
    logger.info('My public IP address is: {}'.format(ip))

    sql = "SELECT * FROM asynctask_server WHERE ip = " + '\'' + str(ip) + '\''
    logger.info(sql)
    print(sql)

    logger.info(sql)
    mysql_cursor.execute(sql)
    server_id = -1
    query_result = mysql_cursor.fetchall()
    for row in query_result:
        logger.info("query result")
        server_id = row[3]
        logger.info("server id %s " % server_id)

    sql = "SELECT * FROM asynctask_api_key WHERE level = '1' and server_id = " + \
        '\'' + str(server_id) + '\''
    mysql_cursor.execute(sql)

    query_result = mysql_cursor.fetchall()

    for row in query_result:
        logger.info("user name is : %s " % str(row[9]))
        logger.info("update followers for %s " % str(row[7]))
        update_followers(row[7])

    mysql_cursor.close()
    mysql_connection.close()


load_level1_keys()
load_level2_keys()
# load_level3_keys()
load_dmcontents()


def get_api_by_key(key):
    auth = tweepy.OAuthHandler(key["CONSUMER_KEY"], key["CONSUMER_SECRET"])
    auth.set_access_token(key["ACCESS_KEY"], key["ACCESS_SECRET"])

    tw_api = tweepy.API(auth, wait_on_rate_limit=True,
                        wait_on_rate_limit_notify=True)
    return tw_api


def get_twitter_api_by_id(api_id):
    for key in level1_keys:
        if key["ID"] == api_id:
            auth = tweepy.OAuthHandler(
                key["CONSUMER_KEY"], key["CONSUMER_SECRET"])
            auth.set_access_token(key["ACCESS_KEY"], key["ACCESS_SECRET"])

            logger.info(key)

            tw_api = tweepy.API(auth, wait_on_rate_limit=True,
                                wait_on_rate_limit_notify=True)
            return (tw_api, key["ID"])
    logger.info("could not find api by id !!!!!!!")
    return None


def get_free_proxy():
    logger.info("into get free proxy")
    mysql_connection = mysql.connect(
        host=HOST, database=DATABASE, user=USER, password=PASSWORD, buffered=True)
    mysql_cursor = mysql_connection.cursor(buffered=True)

    sql = "SELECT * FROM asynctask_proxy WHERE status = 'yes'"
    mysql_cursor.execute(sql)

    query_result = mysql_cursor.fetchall()
    proxies = []
    for row in query_result:
        proxies.append(row[1])
    logger.info("get proxies with len %d " % len(proxies))
    mysql_cursor.close()
    mysql_connection.close()

    if len(proxies) > 0:
        idx = random.randint(0, len(proxies)-1)
        return proxies[idx]
    else:
        return None


def get_twitter_api(level, use_proxy=False):
    logger.info("level %d " % level)
    key_list = []
    key_index = 0
    global level1_index
    global level2_index
    global level3_index
    if level == 1:
        print("111")
        if len(level1_keys) == 0:
            return (None, None)
        key_list = level1_keys
        key_index = level1_index
        level1_index += 1
        level1_index %= len(level1_keys)
        print("level1 keys len ", len(level1_keys))
    elif level == 2:
        print("222")
        key_list = level2_keys
        key_index = level2_index
        if len(level2_keys) == 0:
            return (None, None)
        level2_index += 1
        level2_index %= len(level2_keys)
        print("level2 keys len ", len(level2_keys))
    elif level == 3:
        print("333")
        key_list = level3_keys
        key_index = level3_index
        if len(level3_keys) == 0:
            return (None, None)
        level3_index += 1
        level3_index %= len(level3_keys)
        print("level3 keys len ", len(level3_keys))

    logger.info("get random index %d", key_index)
    key = key_list[key_index]
    auth = tweepy.OAuthHandler(key["CONSUMER_KEY"], key["CONSUMER_SECRET"])
    auth.set_access_token(key["ACCESS_KEY"], key["ACCESS_SECRET"])

    logger.info(key)
    tw_api = None
    free_proxy = get_free_proxy()
    if use_proxy and free_proxy != None:
        logger.info("generate api by proxy %s " % free_proxy)
        tw_api = tweepy.API(auth, proxy=free_proxy, wait_on_rate_limit=True,
                            wait_on_rate_limit_notify=True)
    else:
        tw_api = tweepy.API(auth, wait_on_rate_limit=True,
                            wait_on_rate_limit_notify=True)
    logger.info("generated api")

    return (tw_api, key["ID"])


get_twitter_api(1)
get_twitter_api(2)
# get_twitter_api(3)

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


def record_content_update(table_name, sent_index, reply_index, id, is_reply):
    logger.info("into record content replied %s " % table_name)
    logger.info(is_reply)
    logger.info(id)
    mysql_connection = mysql.connect(
        host=HOST, database=DATABASE, user=USER, password=PASSWORD, buffered=True)
    print("Connected to:", mysql_connection.get_server_info())
    mysql_cursor = mysql_connection.cursor(buffered=True)

    sql = "SELECT * FROM " + table_name + " WHERE id = " + \
        "\"" + str(id) + "\""
    mysql_cursor.execute(sql)

    query_result = mysql_cursor.fetchall()
    sent = 0
    replied = 0
    for row in query_result:
        logger.info(row)
        sent = int(row[sent_index])
        replied = int(row[reply_index])
    logger.info("before replied %d " % replied)
    logger.info("before sent %d " % sent)
    if is_reply:
        replied += 1
        logger.info("update replied with %d " % replied)
        sql = "Update " + table_name + " Set replied = " + \
            str(replied) + " Where id = " + "\"" + str(id) + "\""
    else:
        sent += 1
        logger.info("update sent with %d " % sent)
        sql = "Update " + table_name + " Set sent = " + \
            str(sent) + " Where id = " + "\"" + str(id) + "\""
    mysql_cursor.execute(sql)
    if sent > 0:
        ratio = round((replied*100) / sent, 2)
        ratio_str = str(ratio) + "%"
        logger.info("after replied %d " % replied)
        logger.info("after sent %d " % sent)
        logger.info("ratio %s " % str(ratio_str))
        sql = "Update " + table_name + " Set ratio = \"" + \
            str(ratio_str) + "\" Where id = " + "\"" + str(id) + "\""
        mysql_cursor.execute(sql)

    mysql_connection.commit()
    mysql_cursor.close()
    mysql_connection.close()


def set_api_status(tw_api, error_message, key_id):

    mysql_connection = mysql.connect(
        host=HOST, database=DATABASE, user=USER, password=PASSWORD, buffered=True)
    print("Connected to:", mysql_connection.get_server_info())
    mysql_cursor = mysql_connection.cursor(buffered=True)

    sql = "Update asynctask_api_key Set status = \"" + error_message + "\" Where user_id = " + \
        "\"" + str(key_id) + "\""
    mysql_cursor.execute(sql)

    mysql_connection.commit()
    mysql_cursor.close()
    mysql_connection.close()

    try:
        if error_message != "normal":
            tw_api.send_direct_message("bruce_ywong", error_message)
    except tweepy.TweepError as e:
        print("Tweepy Error: {}".format(e))
        logger.info("Tweepy Error: {}".format(e))


def get_seed_users(key_word):
    print("get seed users")
    logger.info("get seed users on %s " % key_word)
    db_users = mongo_db["seedusers"]
    tw_api, key_id = get_twitter_api(3)
    if tw_api == None:
        return
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
        set_api_status(tw_api, "normal", key_id)
    except tweepy.TweepError as e:
        print("Tweepy Error: {}".format(e))
        logger.info("Tweepy Error: {}".format(e))
        set_api_status(tw_api, format(e), key_id)


def store_followers(ids):
    logger.info("start inserting all into mongo")
    tw_api, key_id = get_twitter_api(2)
    if tw_api == None:
        return
    db_users = mongo_db["users"]
    seed_users = mongo_db["seedusers"]
    count = 0
    for i, uid in enumerate(ids):
        user = tw_api.get_user(uid)
        if user.followers_count > 100:
            if user.followers_count > 5000:
                logger.info("found see user %s " % user.screen_name)
                key = {"id": user.id}
                data = {"screen_name": user.screen_name, "name": user.name, "id": user.id,
                        "follwers": user.followers_count, "location": user.location, "crawled": False}
                seed_users.update(key, data, upsert=True)
            relation = tw_api.show_friendship(target_id=user.id)
            if relation[0].can_dm:
                logger.info("found normal user %s " % user.screen_name)
                key = {"id": user.id}
                data = {"screen_name": user.screen_name, "name": user.name, "id": user.id,
                        "follwers": user.followers_count, "location": user.location, "dmed": False}
                # if user.utc_offset != None:
                #     data["utc_offset"] = user.utc_offset
                # if user.favourites_count != None:
                #     data["favourites_count"] = user.favourites_count
                if user.lang != None:
                    data["lang"] = user.lang
                # if user.status != None:
                #     data["status"] = user.status
                # if user.notifications != None:
                #     data["notifications"] = user.notifications
                # if user.protected != None:
                #     data["protected"] = user.protected
                if user.created_at != None:
                    data["created_at"] = user.created_at
                if user.description != None:
                    data["description"] = user.description
                if user.url != None:
                    data["url"] = user.url
                if user.friends_count != None:
                    data["friends_count"] = user.friends_count

                # tweets = tw_api.user_timeline(screen_name=user.screen_name,
                #                               # 200 is the maximum allowed count
                #                               count=20,
                #                               include_rts=False,
                #                               # Necessary to keep full_text
                #                               # otherwise only the first 140 words are extracted
                #                               tweet_mode='extended'
                #                               )
                # if tweets != None:
                #     data["tweets"] = tweets
                #     logger.info(tweets)

                logger.info("collected data")
                logger.info(data)
                db_users.update(key, data, upsert=True)
                count += 1
                logger.info("found normal user count %d " % count)
                if count == 50:
                    count = 0
                    insert_stat_info(50, 0, 0)

                time.sleep(300)
    insert_stat_info(count, 0, 0)
    logger.info("done inserting all into mongo")


def get_followers(user_name):
    print("Set up Threading get followers of %s" % user_name)
    logger.info("Set up Threading get followers of %s" % user_name)
    logger.info('Number of current threads is %d', threading.active_count())
    logger.info("into get followers >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
    tw_api, key_id = get_twitter_api(2)
    if tw_api == None:
        return
    count = 0
    try:
        logger.info("into get page")
        for page in tweepy.Cursor(tw_api.followers_ids, screen_name=user_name).pages():
            ids = []
            ids.extend(page)
            logger.info("get new page with ids of %d" % len(ids))
            print("get new page with ids of %d" % len(ids))
            store_followers(ids)
            time.sleep(5000)
        set_api_status(tw_api, "normal", key_id)
    except tweepy.TweepError as e:
        print("Tweepy Error: {}".format(e))
        logger.info("Tweepy Error: {}".format(e))
        set_api_status(tw_api, format(e), key_id)
    logger.info("done loading all followers")
    print("done loading all followers")


def send_direct_message(list_of_users, text, content_id, tw_api, is_reply, key_id):

    logger.info("contetn %s " % text)
    logger.info("content id %s " % content_id)
    logger.info("into direct message by %s " % tw_api.me().screen_name)
    logger.info("send direct message >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
    logger.info(list_of_users)

    try:
        for user in list_of_users:
            logger.info("in circle")
            logger.info("send dm to %s " % user['name'])
            firstname = user["name"].split(' ')[0]

            starters = ["Hi", "Hello", "Hey", "Good morning",
                        "Good evening", "Good afternoon"]
            idx = random.randint(0, len(starters)-1)

            message = starters[idx] + " " + firstname + ", " + text
            if is_reply:
                logger.info("it is a reply")
                message = text
            else:
                logger.info("it is a auto message")
                logger.info("sent content id: %s " % content_id)
                users = mongo_db["users"]
                users.update({"id": int(user["id"])}, {
                             "$set": {"content_id": content_id}}, upsert=True)
                logger.info("insert done")
                record_content_update(
                    "asynctask_campaign_content", 6, 5, content_id, False)
                record_content_update(
                    "asynctask_twitter_account", 10, 9, key_id, False)

            direct_message = tw_api.send_direct_message(
                user["id"], message)
#           logger.info("direct message id: " + direct_message.id)
#           tw_api.destroy_direct_message(direct_message.id)
        insert_stat_info(0, len(list_of_users), 0)
        set_api_status(tw_api, "normal", key_id)
    except tweepy.TweepError as e:
        print("Tweepy Error: {}".format(e))
        logger.info("Tweepy Error: {}".format(e))
        set_api_status(tw_api, format(e), key_id)
    logger.info("done threading send DM")


@ api_view(['GET', 'PUT', 'DELETE'])
def get_id_by_name(request, user_name):
    if request.method == 'GET':
        logger.info("find id for %s " % user_name)
        server_request()
        user_name = user_name.strip()
        try:
            tw_api, key_id = get_twitter_api(2, False)
            if tw_api == None:
                return HttpResponse("api key empty")
            user = tw_api.get_user(user_name)

            # fetching the ID
            ID = user.id_str
            logger.info(ID)
            set_api_status(tw_api, "normal", key_id)
            return HttpResponse(str(ID))
        except tweepy.TweepError as e:
            print("Tweepy Error: {}".format(e))
            logger.info("Tweepy Error: {}".format(e))
            error_message = "get_id_by_name " + format(e)
            set_api_status(tw_api, error_message, key_id)
            return HttpResponse(str(format(e)))


@ api_view(['GET', 'PUT', 'DELETE'])
def get_tweet_by_name(request, user_name):
    if request.method == 'GET':
        logger.info("find tweet for %s " % user_name)
        user_name = user_name.strip()
        try:
            tw_api, key_id = get_twitter_api(2, False)
            if tw_api == None:
                return HttpResponse("api key empty")

            tweets = tw_api.user_timeline(screen_name=user_name,
                                          # 200 is the maximum allowed count
                                          count=20,
                                          include_rts=False,
                                          # Necessary to keep full_text
                                          # otherwise only the first 140 words are extracted
                                          tweet_mode='extended'
                                          )
            if tweets != None:
                logger.info("found tweets")
                logger.info(tweets)
                logger.info("length of tweet: %d " % len(tweets))
            result = []
            for tweet in tweets:
                result.append(tweet.full_text)

            return HttpResponse(str(result))
        except tweepy.TweepError as e:
            print("Tweepy Error: {}".format(e))
            logger.info("Tweepy Error: {}".format(e))
            error_message = "get_id_by_name " + format(e)
            set_api_status(tw_api, error_message, key_id)
            return HttpResponse(str(format(e)))


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
        server_request()
        request_body = request.data
        logger.info(request_body)
        user_list = request_body["users"]

        content = request_body["content"]
        content_id = -1
        if len(dm_contents) >= 1 and request_body["isreply"] != True:
            idx = random.randint(0, len(dm_contents)-1)
            content = dm_contents[idx][0]
            content_id = dm_contents[idx][1]
            logger.info("content_id  %s " % content_id)
        tw_api, key_id = get_twitter_api(1)
        if tw_api == None:
            return HttpResponse("api keys empty")
        is_reply = False
        if 'api_id' in request_body.keys():
            logger.info("found api id in request")
            tw_api, key_id = get_twitter_api_by_id(request_body["api_id"])
            is_reply = True
        logger.info(user_list)
        logger.info(content)
        t = threading.Thread(target=send_direct_message,
                             args=(user_list, content, content_id, tw_api, is_reply, key_id))
        t.start()
        logger.info("done DM")
        return HttpResponse("ok")


def store_direct_message(direct_message, sender_name, receiver_name):
    mysql_connection = mysql.connect(
        host=HOST, database=DATABASE, user=USER, password=PASSWORD, buffered=True)
    print("Connected to:", mysql_connection.get_server_info())
    mysql_cursor = mysql_connection.cursor(buffered=True)

    date = datetime.fromtimestamp(int(direct_message.created_timestamp)/1000)
    logger.info(date)
    date_object = date.strftime('%Y-%m-%d %H:%M:%S')
    logger.info(date_object)
    sql = "INSERT ignore INTO asynctask_message (messageid, sender, receiver, type, content, replied, date,sender_name, receiver_name,reply) VALUES (%s, %s,%s,%s,%s,%s,%s,%s,%s,%s)"
    val = (direct_message.id, direct_message.message_create['sender_id'],
           direct_message.message_create['target']['recipient_id'], direct_message.type,
           str(direct_message.message_create['message_data']['text']), "no", date_object, sender_name, receiver_name, " ")

    mysql_cursor.execute(sql, val)

    mysql_connection.commit()

    mysql_cursor.close()
    mysql_connection.close()


def store_direct_message_by_dict(messages):
    mysql_connection = mysql.connect(
        host=HOST, database=DATABASE, user=USER, password=PASSWORD, buffered=True)
    print("Connected to:", mysql_connection.get_server_info())
    mysql_cursor = mysql_connection.cursor(buffered=True)

    for key, value in messages.items():
        sql = "INSERT ignore INTO asynctask_message (messageid, sender, receiver, type, content, replied, date,sender_name, receiver_name,reply,followed,receiver_desc,server_id) VALUES (%s, %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        val = (value["messageid"], value["sender"],
               value["receiver"],  value["type"],
               value["content"],  value["replied"],  value["date"],
               value["sender_name"],  value["receiver_name"],
               value["reply"], value["followed"],
               value["receiver_desc"], value["server_id"])

        mysql_cursor.execute(sql, val)

        mysql_connection.commit()

    mysql_cursor.close()
    mysql_connection.close()


def create_friendship_by_id(userID, tw_api, key_id):
    try:
        tw_api.create_friendship(userID)
        set_api_status(tw_api, "normal", key_id)
    except tweepy.TweepError as e:
        print("Tweepy Error: {}".format(e))
        logger.info("Tweepy Error: {}".format(e))
        error_message = "create_friendship_by_id" + format(e)
        set_api_status(tw_api, error_message, key_id)


def get_trends():
    tw_api, key_id = get_twitter_api(2)
    # trends = tw_api.trends_available()
    # for trend in trends:
    #     logger.info(trend)

    SF_WOE_ID = 2487956
    sf_trends = tw_api.trends_place(SF_WOE_ID)
    trends = json.loads(json.dumps(sf_trends, indent=1))

    for trend in trends[0]["trends"]:
        print(trend["name"])


def get_tweets_by_keyword(key):
    tw_api = get_api_by_key(key)
    search_words = "#Zimbabwe"
    date_since = key["LAST"]
    tweets = tweepy.Cursor(tw_api.search,
                           q=search_words,
                           lang="en",
                           since=date_since).items(10)
    ids = []
    for tweet in tweets:
        logger.info("content %s " % tweet.text)
        logger.info("ID %s " % tweet.id)
        ids.append(tweet.id)
    return ids


def humanize_by_key(key):
    tw_api = get_api_by_key(key)
    ids = get_tweets_by_keyword(key)

    logger.info("get ids back with len %d " % len(ids))
    logger.info(key)
    if len(ids) > 0:
        idx = random.randint(0, len(ids)-1)
        tw_api.retweet(ids[idx])


def get_followers_count_by_id(id):

    tw_api, key_id = get_twitter_api(2)
    if tw_api == None:
        return 0
    followers = 0
    try:
        followers = tw_api.get_user(id).followers_count
    except tweepy.TweepError as e:
        print("Tweepy Error: {}".format(e))
        logger.info("Tweepy Error: {}".format(e))
        error_message = "get_followers_count_by_id" + format(e)
        set_api_status(tw_api, error_message, key_id)
    return followers


def update_followers(id):
    id = str(id)
    logger.info("update followers for %s " % id)
    mysql_connection = mysql.connect(
        host=HOST, database=DATABASE, user=USER, password=PASSWORD, buffered=True)
    print("Connected to:", mysql_connection.get_server_info())
    mysql_cursor = mysql_connection.cursor(buffered=True)

    follower_count = get_followers_count_by_id(id)

    logger.info("the followers : %d " % follower_count)
    sql = "Update asynctask_twitter_account Set followers = \"" + str(follower_count) + "\" Where id = " + \
        "\"" + str(id) + "\""
    mysql_cursor.execute(sql)

    mysql_connection.commit()
    mysql_cursor.close()
    mysql_connection.close()


def get_desc_by_id(user_id):

    mysql_connection = mysql.connect(
        host=HOST, database=DATABASE, user=USER, password=PASSWORD, buffered=True)
    print("Connected to:", mysql_connection.get_server_info())
    mysql_cursor = mysql_connection.cursor(buffered=True)

    sql = "SELECT * FROM asynctask_twitter_account WHERE id = " + \
        "\"" + str(user_id) + "\""
    mysql_cursor.execute(sql)

    query_result = mysql_cursor.fetchall()
    result = ""
    for row in query_result:
        result = row[3]

    mysql_connection.commit()
    mysql_cursor.close()
    mysql_connection.close()

    return result


@ api_view(['GET', 'PUT', 'DELETE'])
def humanize(request):
    if request.method == 'GET':
        logger.info("into humanize")
        keys = []
        for key in level1_keys:
            keys.append(key)
        for key in level2_keys:
            keys.append(key)
        for key in level3_keys:
            keys.append(key)
        for key in keys:
            humanize_by_key(key)
        logger.info("done")
        return HttpResponse("ok")


@ api_view(['GET', 'PUT', 'DELETE'])
def refresh_dmcontents(request):
    logger.info("into load cm contents")
    dm_contents.clear()
    load_dmcontents()
    logger.info("done load cm contents")
    return HttpResponse("ok")


@ api_view(['GET', 'PUT', 'DELETE'])
def get_ip(request):
    if request.method == 'GET':
        ip = get('https://api.ipify.org').text
        logger.info('My public IP address is: {}'.format(ip))
        return HttpResponse(str(ip))


@ api_view(['GET', 'PUT', 'DELETE'])
def crm_manager(request):
    server_request()
    if request.method == 'GET':
        logger.info("into CRM")
    load_level1_keys()
    if len(level1_keys) == 0:
        return HttpResponse(" level1 keys empty")
    logger.info("level1 keys: %d" % len(level1_keys))
    key_ids = []
    for key in level1_keys:
        if key["ID"] in key_ids:
            continue
        key_ids.append(key["ID"])
        logger.info(key)
        db_users = mongo_db["messages"]
        auth = tweepy.OAuthHandler(
            key["CONSUMER_KEY"], key["CONSUMER_SECRET"])
        auth.set_access_token(key["ACCESS_KEY"], key["ACCESS_SECRET"])

        tw_api = tweepy.API(auth, wait_on_rate_limit=True,
                            wait_on_rate_limit_notify=True)

        try:
            direct_messages = tw_api.list_direct_messages()

            logger.info("the number of messages is %d " % len(direct_messages))
            count = 0
            last_timestamp = 0
            messages = dict()
            for direct_message in direct_messages:
                if direct_message.created_timestamp > key['LAST'] and direct_message.message_create['target']['recipient_id'] == key["ID"]:
                    # logger.info(direct_message.created_timestamp)
                    # logger.info("The type is : " + direct_message.type)
                    # logger.info("The id is : " + direct_message.id)
                    # logger.info("The recipient_id is : " +
                    #             direct_message.message_create['target']['recipient_id'])
                    # logger.info("The sender_id is : " +
                    #             direct_message.message_create['sender_id'])
                    # logger.info(
                    #     "The text is : " + str(direct_message.message_create['message_data']['text']))
                    receiver_id = direct_message.message_create['target']['recipient_id']
                    sender_name = tw_api.get_user(
                        direct_message.message_create['sender_id']).screen_name
                    receiver_name = tw_api.get_user(receiver_id).screen_name
                    receiver_desc = get_desc_by_id(receiver_id)
                    logger.info("sender name %s %s " %
                                (sender_name, receiver_name))
                    relation = tw_api.show_friendship(
                        target_id=direct_message.message_create['sender_id'])
                    # logger.info(relation[0])

                    # ******** follow the sender if not
                    follwed = "No"
                    if not relation[0].following and not relation[0].following_requested:
                        create_friendship_by_id(
                            direct_message.message_create['sender_id'], tw_api, key["ID"])
                        logger.info("followed user by id")
                    if relation[0].followed_by:
                        follwed = "Yes"

                    # ********* record the id of the message content that was sent to the users, also mark if they replied
                    users = mongo_db["users"]
                    replied = False
                    content_id = -1
                    query_result = users.find(
                        {"id": int(direct_message.message_create['sender_id'])})
                    for x in query_result:
                        if "replied" in x:
                            replied = x["replied"]
                        if "content_id" in x:
                            content_id = x["content_id"]
                    # logger.info("replied")
                    # logger.info(replied)
                    # logger.info("content id")
                    # logger.info(content_id)

                    # *********** record to stat if this is first time reply, update two tables
                    if not replied and content_id != -1:  # first time reply
                        record_content_update(
                            "asynctask_campaign_content", 6, 5, content_id, True)
                        record_content_update(
                            "asynctask_twitter_account", 10, 9, key["ID"], True)

                    users.update({"id": int(direct_message.message_create['sender_id'])}, {
                        "$set": {"replied": True}}, upsert=True)

                    # store_direct_message(
                    #     direct_message, sender_name, receiver_name)

                    # ************* if there is multiple messages from the same sender, combine them together
                    if sender_name in messages:  # ***** not the first message, only attach content
                        data = messages[sender_name]
                        content = str(
                            direct_message.message_create['message_data']['text']) + ". \n\n\n " + data["content"]
                        data["content"] = content
                    else:
                        data = dict()
                        date = datetime.fromtimestamp(
                            int(direct_message.created_timestamp)/1000)
                        date_object = date.strftime('%Y-%m-%d %H:%M:%S')
                        data["messageid"] = direct_message.id
                        data["sender"] = direct_message.message_create['sender_id']
                        data["receiver"] = direct_message.message_create['target']['recipient_id']
                        data["type"] = direct_message.type
                        data["content"] = str(
                            direct_message.message_create['message_data']['text'])
                        data["replied"] = "no"
                        data["date"] = date_object
                        data["sender_name"] = sender_name
                        data["receiver_name"] = receiver_name
                        data["receiver_desc"] = receiver_desc
                        data["reply"] = " "
                        data["followed"] = follwed
                        data["server_id"] = key["SERVER_ID"]

                        messages[sender_name] = data
                        count += 1
                    last_timestamp = max(int(last_timestamp), int(
                        direct_message.created_timestamp))
            logger.info("final last time stamp %s " % last_timestamp)
            logger.info(messages)
            store_direct_message_by_dict(messages)
            if last_timestamp != 0:
                insert_last_reply(key['ID'], last_timestamp)

            insert_stat_info(0, 0, count)
        except tweepy.TweepError as e:
            print("Tweepy Error: {}".format(e))
            logger.info("Tweepy Error: {}".format(e))
            set_api_status(tw_api, format(e), key["ID"])
        update_followers(key["ID"])

    # load_level2_keys()
    # for key in level2_keys:
    #     update_followers(key["ID"])
    logger.info("done CRM")
    return HttpResponse("ok")


@ api_view(['GET', 'PUT', 'DELETE'])
def refresh_api(request):
    if request.method == 'GET':
        logger.info("start refresh api")
        load_level1_keys()
        load_level2_keys()
        # load_level3_keys()

        logger.info("done refresh api")

        logger.info("into load cm contents")
        dm_contents.clear()
        load_dmcontents()
        logger.info("done load cm contents")

        logger.info("into refresh followers")

        refresh_followers()
        logger.info("done refresh followers")
        return HttpResponse("ok")
