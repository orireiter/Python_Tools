from MySQL_Class import MSQL

db = MSQL(host_ip='localhost', username='root', password='123456')

print(db.create_db('test_db'))
print(db.create_table('test_table', '(id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255), address VARCHAR(255), age INT)'))
print(db.show_dbs())


print(db.show_columns('test_table'))
print(db.insert_record('test_table', ('name',), ('bob',)))
print(db.delete_record('test_table', '1'))
print(db.insert_record('test_table', ('name', 'age'), ('bob', 22)))
print(db.insert_record('test_table', ('name', 'age', 'address'), ('bob', 56, 'new york')))
print(db.find_record('test_table', 2))

print(db.find_records('test_table', 'name', 'bob'))

print(db.update_record('test_table',('name',), ('john',),'2'))
print(db.update_record('test_table',('name', 'age'), ('rob', 11),'3'))
