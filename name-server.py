from socket import socket
from multiprocessing import Process
from sys import stdout
from os import chdir, getpid
import sqlite3
import string
import random
from hashlib import pbkdf2_hmac as password_hash
from secrets import token_bytes, token_hex



# HELPER FUNCTIONS

def log(string):
	print("%06d | %s" % (getpid(), string))
	stdout.flush()



# CONSTANTS

PORT = 1234
ROOT_FOLDER = '.'
DATABASE = 'database.db'



# INITIAL DATABASE CONFIGURATION

db_conn = sqlite3.connect(DATABASE, isolation_level=None) # autocommits = on
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



# RECEIVING DATA FUNCTIONS

def get_data(conn, len):
	return conn.recv(len)

def get_int(conn, len=1):
	data = get_data(conn, len)
	result = 0
	for byte in data:
		result << 8
		result += byte
	return result

def get_fixed_len_string(conn, len):
	return get_data(conn, len).decode('ascii').rstrip('\x00')

def get_var_len_string(conn, length_of_lenght = 1):
	return get_fixed_len_string(conn, get_int(conn, length_of_lenght))



# SENDING DATA FUNCTIONS

def return_status(conn, code, message=''):
	log('Returned %02x %s' % (code, message))
	l = len(message)
	conn.send(bytes([code, l//256, l%256]))
	conn.send(message.encode('utf-8'))
	conn.close()

def return_token(conn, login):
	token = token_bytes(32)
	db_cursor.execute("INSERT INTO tokens (login, token) VALUES (?, ?);", (login, sqlite3.Binary(token)))
	conn.send(b'\x00')
	conn.send(token)
	conn.close()



# MAIN FUNCTION

def handle_client(conn, addr):
	log('Got connection from {}'.format(addr))
	id = get_int(conn);

	if False: # for alligning conditions below
		pass
	elif (id == 0x00): # logout
		token = get_data(conn, 32)
		db_cursor.execute("DELETE FROM users WHERE token = ?;", (token,))
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
		token = get_data(conn, 32)

	elif (id == 0x04): # file create
		token = get_data(conn, 32)
		filename = get_var_len_string(conn)

	elif (id == 0x05): # file read
		token = get_data(conn, 32)
		filename = get_var_len_string(conn)

	elif (id == 0x06): # file write
		token = get_data(conn, 32)
		filename_len = get_int(conn)
		size = get_int(conn, 4)
		filename = get_fixed_len_string(conn, filename_len)

	elif (id == 0x07): # file delete
		token = get_data(conn, 32)
		filename = get_var_len_string(conn)

	elif (id == 0x08): # file info
		token = get_data(conn, 32)
		filename = get_var_len_string(conn)

	elif (id == 0x09): # file copy
		token = get_data(conn, 32)
		source_len = get_int(conn)
		destination_len = get_int(conn)
		source = get_fixed_len_string(conn, source_len)
		destination = get_fixed_len_string(conn, destination_len)

	elif (id == 0x0A): # file move
		token = get_data(conn, 32)
		source_len = get_int(conn)
		destination_len = get_int(conn)
		source = get_fixed_len_string(conn, source_len)
		destination = get_fixed_len_string(conn, destination_len)

	elif (id == 0x0B): # open dir, deprecated
		pass # TODO: error

	elif (id == 0x0C): # directory read
		token = get_data(conn, 32)
		dir = get_var_len_string(conn)

	elif (id == 0x0C): # directory make
		token = get_data(conn, 32)
		dir = get_var_len_string(conn)

	elif (id == 0x0C): # directory delete
		token = get_data(conn, 32)
		dir = get_var_len_string(conn)

	else: # unknown id
		pass # TODO: return error

	# stdout.flush()



# START ACCEPTING CONNECTIONS

if __name__ == '__main__':
	s = socket()
	s.bind(('', PORT))
	s.listen()

	log('Server started!')
	log('Waiting for clients...')

	while True:
		conn, addr = s.accept()	 # Establish connection with client.
		p = Process(target = handle_client, args = (conn, addr))
		p.start()
		#p.join()
		pass

	s.close()
