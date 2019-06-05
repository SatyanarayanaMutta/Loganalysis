# !/usr/bin/env python


import psycopg2


# Database Name


db_sql = "news"


# Three Solutions Headings

qution_1_info = "\n\n"+(6*'='+'>')+"1) The All time popular 3  "
qution_1_info += "Articals are:\n"
qution_2_info = "\n\n\n"+(6*'='+'>')+"2) The Most popular "
qution_2_info += "Artical Authors are:\n"
qution_3_info = "\n\n\n"+(6*'='+'>')+"3) Days with more than 1%"
qution_3_info += " of errors are:\n"


# First Problem ======> Tree most Popular Articals? <======
# Solution Query of First Problem


top3articals_q = '''SELECT title, count(*) as articals_num FROM
articles JOIN log ON articles.slug = substring(log.path, 10)
GROUP BY title ORDER BY articals_num DESC LIMIT 3;'''


# Second Problem ======> Most popular all time Artical Authors? <======
# Solution Query of Second Problem


top_authors_q = '''select authors.name, count(*) as authors_num
from articles, authors, log where log.status='200 OK'
and authors.id = articles.author and articles.slug = substr(log.path, 10)
group by authors.name order by authors_num desc;'''

# Third Problem ======> Which Days with more than 1% of errors? <======
# Solution Query of Third Problem


errors_q = '''select * from (select x.days_count ,
round(cast((100*y.hits) as numeric) / cast(x.hits as numeric) , 2)
as error_per from (select date(time) as days_count , count(*) as hits
from log group by days_count) as x inner
join (select date(time) as days_count, count(*) as hits from log
where status like '%404%' group by days_count) as y
on x.days_count = y.days_count) as t where error_per > 1.0;'''


# Connect PostgreSQL with database file
# ---> if sucessfully connected then display "Connection Success"
# ---> otherwise display "Connection ERROR"


def db_connect(dbname="news"):
    try:
        db_obj = psycopg2.connect("dbname = {}".format(dbname))
        db_cur = db_obj.cursor()
        print("\n Database" + "  <==================>  " + "\
Python")
        return db_obj, db_cur
    except Exception as e:
        print(e, "\nConnection ERROR Please try again")


# Display Top three Articals at all time

def top_articles():
    query1 = top3articals_q
    db_cur.execute(query1)
    result = db_cur.fetchall()
    print(qution_1_info)
    for i in range(0, len(result), 1):
        print(12 * '-'+'> ' + result[i][0] + " - " + str(result[i][1]) + "\
 Views")


# Display all time Most popular authors

def top_authors():
    query2 = top_authors_q
    db_cur.execute(query2)
    result = db_cur.fetchall()
    print(qution_2_info)
    for i in range(0, len(result), 1):
        print(12 * '-' + '> ' + result[i][0] + " - " + str(result[i][1]) + "\
 Views")


# Display days on which more than 1% of requests lead to errors

def log_error():
    query3 = errors_q
    db_cur.execute(query3)
    result = db_cur.fetchall()
    print(qution_3_info)
    print(12 * '-'+'> ' + str(result[0][0]) + "  =  " + str(result[0][1]) + '\
% Errors\n\n')

# All problems Solutions displaying started from here


if __name__ == '__main__':
    db_obj, db_cur = db_connect(db_sql)
    top_articles()
    top_authors()
    log_error()
    print("\n Three Solutions output printing completed.")

# Terminate connections between news.sql and this python file

db_cur.close()
db_obj.close()
print("\nDatabase" + "  ========>  X  <========  " + "Python\n")
