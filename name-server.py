from socket import socket, timeout as socket_error
from threading import Thread, get_ident
from sys import stdout
from os import system
import sqlite3
from hashlib import pbkdf2_hmac as password_hash
from secrets import token_bytes, token_hex
from time import sleep
from ipaddress import ip_address



# CONSTANTS

PORT = 1234
ROOT_FOLDER = '/files/'
DATABASE = 'database.db'
PING_DELAY = 60
PING_TIMEOUT = 5



# INITIAL DATABASE CONFIGURATION

db_conn = sqlite3.connect(
	DATABASE,
	check_same_thread = False
) 
db_cursor = db_conn.cursor()

db_cursor.execute('''
    CREATE TABLE IF NOT EXISTS users (
        id           INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
        login        VARCHAR(20)  UNIQUE               NOT NULL,
        password     BLOB(16)                          NOT NULL,
        salt         BLOB(5)                           NOT NULL
    );
''')

db_cursor.execute('''
    CREATE TABLE IF NOT EXISTS tokens (
        id           INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
        login        VARCHAR(20)                       NOT NULL,
        token        BLOB(32)     UNIQUE               NOT NULL,
        FOREIGN KEY (login) REFERENCES users (login)
    );
''')

db_cursor.execute('''
    CREATE TABLE IF NOT EXISTS file_structure (
        id           INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
        login        VARCHAR(20)                       NOT NULL,
        path         VARCHAR(256)                      NOT NULL,
        size         INTEGER(4),
        FOREIGN KEY (login) REFERENCES users (login),
        UNIQUE (path, size)
    );
''')

db_cursor.execute('''
    CREATE TABLE IF NOT EXISTS servers (
        id           INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
        ip           INTEGER(4)                        NOT NULL,
        port         INTEGER(2)                        NOT NULL
    );
''')

db_cursor.execute('''
    CREATE TABLE IF NOT EXISTS files_on_servers (
        id           INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
        file_id      INTEGER                           NOT NULL,
        server_id    INTEGER                           NOT NULL,
        FOREIGN KEY (file_id) REFERENCES file_structure (id),
        FOREIGN KEY (server_id) REFERENCES servers (id)
    );
''')

db_conn.commit()



# HELPER FUNCTIONS

def log(string):
	print("%06d | %s" % (get_ident(), string))
	stdout.flush()

def get_function_by_addr(addr):
	if addr[0] == '25.108.175.17':
		return handle_storage_server
	else:
		return handle_client

def foreach_storage_server(func, additional_params=(), delays=False):
	storage_servers_list_copy = storage_servers_list.copy()
	errors = set()

	for server in storage_servers_list_copy:
		res = func(server, *additional_params)
		if not res:
			storage_servers_list.remove(server)
			errors.add(server)
			if delays: sleep(PING_DELAY / len(storage_servers_list_copy))
	if delays: sleep(PING_DELAY / len(storage_servers_list_copy))

	return errors

def get_folder(login, path=''):
	if path == '':
		return '%s%s' % (ROOT_FOLDER, login)
	else:
		return '%s%s/%s' % (ROOT_FOLDER, login, path)

def is_valid_filename(string):
	# check special conditions
	if	(string == '' or
		string[0] == ' ' or
		string[-1] == ' ' or
		string[-1] == '.'):
			return False
	
	# check each prohibited char
	for c in "\x00\\\"/:*<>|?":
		if c in string:
			return False

	# all good
	return True



# RECEIVING DATA FUNCTIONS

def get_data(conn, len):
	return conn.recv(len)

def get_int(conn, len=1):
	data = get_data(conn, len)
	result = 0
	for byte in data:
		result <<= 8
		result |= byte
	return result

def get_fixed_len_string(conn, len):
	return get_data(conn, len).decode('utf-8').rstrip('\x00')

def get_var_len_string(conn, length_of_lenght = 1):
	return get_fixed_len_string(conn, get_int(conn, length_of_lenght))

def get_login(conn):
	token = get_data(conn, 32)
	db_cursor.execute("SELECT login FROM tokens WHERE token = ?", (token,))
	row = db_cursor.fetchone()
	if row == None:
		return_status(conn, 0x15)
		return None
	else:
		return row[0]



# RESPONSE FUNCTIONS

def return_status(conn, code, message=''):
	log('Returned status code %02x with message "%s"' % (code, message))
	l = len(message)
	conn.send(bytes([code, l//256, l%256]))
	if (isinstance(message, str)):
		message = message.encode('utf-8')
	conn.send(message)
	conn.close()

def return_token(conn, login):
	log('Successfull login/registration as "%s"' % login)
	token = token_bytes(32)
	db_cursor.execute("INSERT INTO tokens (login, token) VALUES (?, ?);", (login, sqlite3.Binary(token)))
	if db_cursor.rowcount == 1:
		db_conn.commit()
		conn.send(b'\x00')
		conn.send(token)
		conn.close()
	else:
		return_status(conn, 0x10) # Unknown auth error

def storage_server_response(conn, code):
	log('Returned status code %02x' % code)
	conn.send(bytes([code]))
	conn.close()



# STORAGE SERVER QUERIES

def server_send(server, data):
	try:
		conn = socket()
		conn.settimeout(PING_TIMEOUT)
		conn.connect(server)
		for d in data:
			conn.send(d)
		return (get_int(conn) == 0)
	except socket_error as e: 
		log('Could not connect to {}'.format(server))
		return False
	finally:
		conn.close()

def server_eval(server, cmd):
	return server_send(server, [b'\x03', bytes([len(cmd)]), cmd.encode('utf-8')])

def server_ping(server, _):
	log('Send ping to {}'.format(server))
	return server_send(server, [b'\x04'])

def server_create_dir(server, login, path=''):
	return server_eval(server, "os.makedirs('%s')" % get_folder(login, path))

def server_remove_dir(server, login, path=''):
	return server_eval(server, "rmtree('%s')" % get_folder(login, path))

def server_create_file(server, login, path):
	return server_eval(server, "open('%s', 'a').close()" % get_folder(login, path))

def server_initialize(server, login, new_user=False):
	if not new_user:
		res1 = server_remove_dir(server, login)
		if not res1: return False
	res2 = server_create_dir(server, login)
	return res2



# HANDLE CLIENT

def initialize(conn, login, new_user=False):
	db_cursor.execute("DELETE FROM file_structure WHERE login = ?;", (login,))
	db_cursor.execute("INSERT INTO file_structure (login, path) VALUES (?, ?);", (login, '/'))
	if db_cursor.rowcount == 1:
		db_conn.commit()
		foreach_storage_server(server_initialize, (login, new_user))
		if new_user:
			return_token(conn, login)
		else:
			return_status(conn, 0x00)
	else:
		return_status(conn, 0x30)

def handle_client(conn, addr):
	log('Got connection from client {}'.format(addr))
	id = get_int(conn)

	if False: # for aligning conditions below
		pass

	elif (id == 0x00): # logout
		token = get_data(conn, 32)
		db_cursor.execute("DELETE FROM tokens WHERE token = ?;", (token,))
		db_cursor.execute("SELECT token FROM tokens WHERE token = ?;", (token,))
		if db_cursor.fetchone() == None:
			db_conn.commit()
			return_status(conn, 0x00)
		else:
			return_status(conn, 0x10)

	elif (id == 0x01): # register
		login = get_fixed_len_string(conn, 20)
		password = get_var_len_string(conn)
		if (not is_valid_filename(login)):
			return_status(conn, 0x12) # Invalid username during registration
		else:
			db_cursor.execute("SELECT login FROM users WHERE login = ?", (login,))
			if db_cursor.fetchone():
				return_status(conn, 0x11) # Username already registered
			else:
				# creating user
				salt = token_bytes(5)
				hashed_password = password_hash('sha256', password.encode('utf-8'), salt, 100000)
				db_cursor.execute("INSERT INTO users (login, password, salt) VALUES (?, ?, ?);", (login, sqlite3.Binary(hashed_password), sqlite3.Binary(salt)))
				if db_cursor.rowcount == 1:
					db_conn.commit()
					initialize(conn, login, new_user=True)
				else:
					return_status(conn, 0x10)

	elif (id == 0x02): # login
		login = get_fixed_len_string(conn, 20)
		password = get_var_len_string(conn)

		db_cursor.execute("SELECT password, salt FROM users WHERE login = ?;", (login,))
		row = db_cursor.fetchone()
		if not row:
			return_status(conn, 0x13) # No such user
		else:
			hashed_password_from_db, salt = row
			hashed_password = password_hash('sha256', password.encode('utf-8'), salt, 100000)
			if (hashed_password != hashed_password_from_db):
				return_status(conn, 0x14)
			else:
				return_token(conn, login)

	elif (id == 0x03): # initialize
		login = get_login(conn)
		initialize(conn, login)

	elif (id == 0x04 or id == 0x0C): # file/directory create
		login = get_login(conn)
		path = get_var_len_string(conn)
		folder, _, filename = path.rpartition('/')
		creating_dir = (id == 0x0C)

		db_cursor.execute("SELECT size FROM file_structure WHERE login = ? AND path = ?;", (login, path))
		row = db_cursor.fetchone()
		if row != None:
			return_status(conn, 0x32 if row[0] == None else 0x22) # Directory/File already exists
		else:
			db_cursor.execute("SELECT path FROM file_structure WHERE size IS NULL AND login = ? AND path = ?;", (login, folder))
			if db_cursor.fetchone() != None:
				return_status(conn, 0x31) # Directory does not exist
			elif not is_valid_filename(filename):
				return_status(conn, 0x33 if creating_dir else 0x24) # Prohibited directory/file name
			else:
				# finally, creating file/directory
				db_cursor.execute("INSERT INTO file_structure (login, path, size) VALUES (?, ?, ?);", (login, path, None if creating_dir else 0))
				if db_cursor.rowcount == 1:
					db_conn.commit()
					if creating_dir:
						foreach_storage_server(server_create_dir, (login, path))
					else:
						# create file on some servers
						db_cursor.execute('''
							SELECT ip, port, COALESCE(sum(size), 0) as mem
							FROM
								(servers as s LEFT JOIN files_on_servers as fs ON s.id = fs.server_id)
								LEFT JOIN file_structure as f ON f.id = fs.file_id
							GROUP BY ip
							ORDER BY mem ASC 
							LIMIT 2
						''')
						for row in db_cursor.fetchall():
							server = (str(ip_address(row[0])), row[1])
							server_create_file(server, login, path)
					return_status(conn, 0x00)
				else:
					return_status(conn, 0x30 if creating_dir else 0x20) # Unknown directory/file error

	elif (id == 0x05): # file read
		login = get_login(conn)
		filename = get_var_len_string(conn)

	elif (id == 0x06): # file write
		login = get_login(conn)
		filename_len = get_int(conn)
		size = get_int(conn, 4)
		filename = get_fixed_len_string(conn, filename_len)

	elif (id == 0x07 or id == 0x0D): # file/directory delete
		login = get_login(conn)
		filename = get_var_len_string(conn)
		deleting_dir = (id == 0x0D)

		db_cursor.execute("SELECT size FROM file_structure WHERE login = ? AND path = ?;", (login, path))
		row = db_cursor.fetchone()
		if (row == None) or (deleting_dir != (row[0] == None)):
			return_status(conn, 0x31 if deleting_dir else 0x21) # Directory/File does not exist
		else:
			# finally, deleting file/directory
			db_cursor.execute("DELETE FROM file_structure WHERE login = ? AND path = ?;", (login, path))
			if db_cursor.rowcount == 1:
				db_conn.commit()
				if deleting_dir:
					foreach_storage_server(server_remove_dir, login)
				else:
					pass # TODO!!! Delete file from servers
				return_status(conn, 0x00)
			else:
				return_status(conn, 0x30 if deleting_dir else 0x20) # Unknown directory/file error


	elif (id == 0x08): # file info
		login = get_login(conn)
		filepath = get_var_len_string(conn)

		db_cursor.execute("SELECT size FROM file_structure WHERE size IS NOT NULL AND login = ? AND path = ?;", (login, filepath))
		if row == None:
			return_status(conn, 0x21) # File does not exist
		else:
			return_status(conn, 0x00, int.to_bytes(row[0], 4, 'big'))

	elif (id == 0x09): # file copy
		login = get_login(conn)
		source_len = get_int(conn)
		destination_len = get_int(conn)
		source = get_fixed_len_string(conn, source_len)
		destination = get_fixed_len_string(conn, destination_len)

	elif (id == 0x0A): # file move
		login = get_login(conn)
		source_len = get_int(conn)
		destination_len = get_int(conn)
		source = get_fixed_len_string(conn, source_len)
		destination = get_fixed_len_string(conn, destination_len)

	elif (id == 0x0B): # directory read
		login = get_login(conn)
		dirname = get_var_len_string(conn)

	#     id == 0x0C   # look above
	#     id == 0x0D   # look above

	else: # unknown id
		return_status(conn, 0x81)

	log('Thread end')



# HANDLE STORAGE SERVER

storage_servers_list = set()

def handle_storage_server(conn, addr):
	log('Got connection from storage server {}'.format(addr))
	id = get_int(conn)

	if False: # for aligning conditions below
		pass

	elif (id == 0x00): # new storage server
		port = get_int(conn, 2)
		storage_servers_list.add((addr[0], port))
		db_cursor.execute("INSERT INTO servers (ip, port) VALUES (?, ?)", (int(ip_address(addr[0])), port))
		db_conn.commit()
		# TODO: tell full folder structure to storage server
		storage_server_response(conn, 0x00)

	elif (id == 0x01): # report
		operation = get_int(conn)
		entity_type = get_int(conn)
		name = get_var_len_string(conn)
		# TODO: change in the database
		storage_server_response(conn, 0x00)

	else: # unknown id
		return_status(conn, 0x81)

	log('Thread end')



# CHECK AVAILABILITY OF STORAGE SERVERS

def ping_storage_servers():
	while True:
		if len(storage_servers_list) == 0:
			sleep(PING_DELAY)
		else:
			dead_servers = foreach_storage_server(
				server_ping, 
				delays = True
			)
			# TODO: check dead servers, reupload files

ping_thread = Thread(target = ping_storage_servers)
ping_thread.start()
#ping_thread.join()



# START ACCEPTING CONNECTIONS

s = socket()
s.bind(('', PORT))
s.listen()

log('Server started!')
log('Waiting for connections...')

while True:
	conn, addr = s.accept()
	user_thread = Thread(target = get_function_by_addr(addr), args = (conn, addr))
	user_thread.start()
	#user_thread.join()

s.close()
