import constants
import csv
import MySQLdb
import time


def run(filepath):
    db = MySQLdb.connect(user=constants.mysql_user, passwd=constants.mysql_password,
            db=constants.mysql_database, charset=constants.mysql_charset)
    db.autocommit(True)

    cursor = db.cursor()

    cursor.execute(""" delete from anchors """)

    delayed_anchor_insertions = set()
    delayed_industry_insertions = set()

    fh = csv.DictReader(open(filepath, 'rb'), delimiter=',')

    for row in fh:

        delayed_industry_insertions.add(row['Industry'].lower())

        row['Twitter Account'] = row['Twitter Account'].lstrip('@').lower()

        delayed_anchor_insertions.add((row['Endorses which candidate'], row['Twitter Account'], row['Industry']))


    cursor.executemany(""" insert ignore into industries (industry)
        values (%s) """, delayed_industry_insertions)

    cursor.executemany(""" insert ignore into anchors (account_id, vote, industry_id)
        select a.account_id, %s, i.industry_id from accounts a, industries i
        where a.account_name like %s and i.industry like %s """, delayed_anchor_insertions)


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(
        description="Push raw anchor csv file into database")
    parser.add_argument('-file', type=str, required=True,
        help='Provide a path to file to push.')
    args = vars(parser.parse_args())

    start = time.time()
    run(args['file'])
    end = time.time()
    print "Runtime: %f seconds" % (end - start)
