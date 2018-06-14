from pyhive import hive
import MySQLdb
conn = hive.Connection(host='localhost', port=10000, username='root', database='test')
cursor = conn.cursor()
cursor.execute('select * from ct_data')
count = 0;gprs_erro_count = 0.0
for i in cursor.fetchall():
  count += 1
  gprs_erro_count += i[12]

average = gprs_erro_count / count

db = MySQLdb.connect("localhost", "root", "123456", "test", charset='utf8' )
cursor = db.cursor()
sql = "insert into ct_data_out(user_count ,gprs_error_average) values ('%f', '%f')" % (count, average)
try:
    cursor.execute(sql)
    db.commit()
except:
    db.rollback()
    
db.close()
print "Done"
        
#print "The user'counts:" ,count
#print "The gprs_erro'average:" , (gprs_erro_count / count)

