from socket import socket, timeout as socket_error
from threading import Thread, get_ident
from sys import stdout
from os import system
import sqlite3
from hashlib import pbkdf2_hmac as password_hash
from secrets import token_bytes, token_hex
from time import sleep



# CONSTANTS

PORT = 1234
ROOT_FOLDER = '/files/'
DATABASE = 'database.db'
PING_DELAY = 60
PING_TIMEOUT = 5



# INITIAL DATABASE CONFIGURATION

db_conn = sqlite3.connect(
	DATABASE,
	isolation_level = None, # enable autocommits
	check_same_thread = False
) 
db_cursor = db_conn.cursor()

db_cursor.execute('''
    CREATE TABLE IF NOT EXISTS users (
       id           INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
       login        VARCHAR(20) UNIQUE                NOT NULL,
       password     BLOB(16)                          NOT NULL,
       salt         BLOB(5)                           NOT NULL
    );
''')

db_cursor.execute('''
    CREATE TABLE IF NOT EXISTS tokens (
       id           INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
       login        VARCHAR(20)                       NOT NULL,
       token        BLOB(32)    UNIQUE                NOT NULL
    );
''')



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
		res = func(server, additional_params)
		if not res:
			storage_servers_list.remove(server)
			errors.add(server)
			if delays: sleep(PING_DELAY / len(storage_servers_list_copy))
	if delays: sleep(PING_DELAY / len(storage_servers_list_copy))

	return errors

def get_folder(login, path='/'):
	return '%s%s%s' % (ROOT_FOLDER, login, path)



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



# CLIEN RESPONSE FUNCTIONS

def return_status(conn, code, message=''):
	log('Returned status code %02x with message "%s"' % (code, message))
	l = len(message)
	conn.send(bytes([code, l//256, l%256]))
	conn.send(message.encode('utf-8'))
	conn.close()

def return_token(conn, login):
	log('Successfull login/registration as "%s"' % login)
	token = token_bytes(32)
	db_cursor.execute("INSERT INTO tokens (login, token) VALUES (?, ?);", (login, sqlite3.Binary(token)))
	conn.send(b'\x00')
	conn.send(token)
	conn.close()

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

def server_create_dir(server, login):
	return server_eval(server, "os.makedirs('%s')" % get_folder(login))

def server_remove_dir(server, login):
	return server_eval(server, "rmtree('%s')" % get_folder(login))

def server_initialize(server, login):
	res1 = server_remove_dir(server, login)
	if not res1: return False
	res2 = server_create_dir(server, login)
	return res2



# HANDLE CLIENT

def handle_client(conn, addr):
	log('Got connection from client {}'.format(addr))
	id = get_int(conn)

	if False: # for aligning conditions below
		pass

	elif (id == 0x00): # logout
		token = get_data(conn, 32)
		db_cursor.execute("DELETE FROM tokens WHERE token = ?;", (token,))
		return_status(conn, 0x00)

	elif (id == 0x01): # register
		login = get_fixed_len_string(conn, 20)
		password = get_var_len_string(conn)
		if (not login.isalnum()):
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
				foreach_storage_server(server_initialize, login)
				return_token(conn, login)

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
		foreach_storage_server(server_initialize, login)

	elif (id == 0x04): # file create
		login = get_login(conn)
		filename = get_var_len_string(conn)

	elif (id == 0x05): # file read
		login = get_login(conn)
		filename = get_var_len_string(conn)

	elif (id == 0x06): # file write
		login = get_login(conn)
		filename_len = get_int(conn)
		size = get_int(conn, 4)
		filename = get_fixed_len_string(conn, filename_len)

	elif (id == 0x07): # file delete
		login = get_login(conn)
		filename = get_var_len_string(conn)

	elif (id == 0x08): # file info
		login = get_login(conn)
		filename = get_var_len_string(conn)

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

	elif (id == 0x0B): # open dir, deprecated
		pass # TODO: error

	elif (id == 0x0C): # directory read
		login = get_login(conn)
		dirname = get_var_len_string(conn)

	elif (id == 0x0D): # directory make
		login = get_login(conn)
		dirname = get_var_len_string(conn)
		foreach_storage_server(server_create_dir, login)

	elif (id == 0x0E): # directory delete
		login = get_login(conn)
		dirname = get_var_len_string(conn)
		foreach_storage_server(server_remove_dir, login)

	else: # unknown id
		pass # TODO: return error

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
		# TODO: tell full folder structure to storage server
		storage_server_response(conn, 0x00)

	elif (id == 0x01): # report
		operation = get_int(conn)
		entity_type = get_int(conn)
		name = get_var_len_string(conn)
		# TODO: change in the database
		storage_server_response(conn, 0x00)

	else: # unknown id
		pass # TODO: return error

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
