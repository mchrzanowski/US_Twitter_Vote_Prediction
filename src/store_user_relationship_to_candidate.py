import constants
import MySQLdb
import time
import twython


def insert_data_into_candidate_following_table(cursor, data):
    cursor.executemany(""" insert ignore into candidate_following
        (account_id, following_obama, following_biden,
        following_romney, following_ryan) values
        (%s, %s, %s, %s, %s) """, data)


def run(single_user=None, force_reupdate=False):

    db = MySQLdb.connect(user=constants.mysql_user,
        passwd=constants.mysql_password,
        db=constants.mysql_database, charset=constants.mysql_charset)
    db.autocommit(True)

    cursor = db.cursor()

    api = twython.Twython(app_key=constants.consumer_key,
        app_secret=constants.consumer_secret,
        oauth_token=constants.access_token_key,
        oauth_token_secret=constants.access_token_secret)

    candidate_names = ('barackobama', 'joebiden', 'mittromney', 'paulryanvp')

    TRANSACTION_BATCH_SIZE = 20

    if single_user != None:
        if force_reupdate:
            cursor.execute(""" delete c from candidate_following c, accounts a
                where c.account_id = a.account_id and a.account_name = %s """,
                single_user)
        cursor.execute(""" select account_name, account_id from accounts
            where account_name = %s """, single_user)

    else:
        if force_reupdate:
            cursor.execute(""" delete from candidate_following """)
            cursor.execute(""" select account_name, account_id
                from accounts """)
        else:
            cursor.execute(""" select a.account_name, a.account_id
                from accounts a
                where a.account_id not in
                (select account_id from candidate_following) """)

    insertion_data = set()

    for row in cursor.fetchall():
        account_name = row[0]
        account_id = row[1]
        print "Working on: %s" % account_name

        following = {'id': account_id}

        for candiate in candidate_names:

            if account_name.lower() == candiate:
                following[candiate] = True
            else:
                relationship = None
                while relationship == None:
                    try:
                        relationship = api.showFriendship(
                            source_screen_name=account_name,
                            target_screen_name=candiate)
                    except twython.TwythonError as te:
                        print te
                        if 'determine source user' in str(te):
                            print "Moving on."
                            break
                        # retry if the problem is Twitter's
                        # rate limit. move on otherwise.
                        elif 'limit' in str(te):
                            print "Sleeping for %s seconds." % \
                                constants.API_COOL_OFF_SECONDS
                            time.sleep(constants.API_COOL_OFF_SECONDS)
                        else:
                            print "Moving on."
                            break
                    except Exception as e:
                        print "This person broke the API. Remove, please."
                        print "Exception: %s" % e
                        break

                if relationship == None:
                    break

                is_following = relationship['relationship']['source']['following']
                following[candiate] = is_following

        if len(following) != 1 + len(candidate_names):
            continue

        insertion_data.add((following['id'], following['barackobama'],
            following['joebiden'], following['mittromney'],
            following['paulryanvp']))

        if len(insertion_data) > TRANSACTION_BATCH_SIZE:
            insert_data_into_candidate_following_table(cursor, insertion_data)
            insertion_data.clear()

    if len(insertion_data) > 0:
        insert_data_into_candidate_following_table(cursor, insertion_data)
        insertion_data.clear()

if __name__ == '__main__':
    import argparse
    start = time.time()

    parser = argparse.ArgumentParser(description="""Check whether accounts
        follow the candidates.""")
    parser.add_argument('-user', type=str, help='Check single user.')
    parser.add_argument('--force_update', action='store_true',
        help='Force update of relationship status.')

    args = vars(parser.parse_args())

    run(args['user'], args['force_update'])

    end = time.time()
    print "Runtime: %f seconds." % (end - start)
