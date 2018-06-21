import pymysql
import time

from impala.dbapi import connect
start = time.time()
conn = connect(host='slave2', port=21050)
cursor1 = conn.cursor()
cursor1.execute('select * from test.ct_data where gprs_error>(select avg(gprs_error)+3*stddev(gprs_error) from test.ct_data) order by gprs_error')
result = cursor1.fetchall()
db = pymysql.connect("localhost", "root", "123456", "test", charset='utf8')
cursor = db.cursor()
for i in result:
    sql = "insert into ct_out(user_id,room_id) values (%f,%f)" % (i[0],i[1])
    try:
        cursor.execute(sql)
        db.commit()
    except:
        db.rollback()

db.close()
print "Done"
end  = time.time()
print("Time:") , (end - start)