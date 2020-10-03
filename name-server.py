from socket import socket
from multiprocessing import Process
from sys import stdout
from os import chdir
import sqlite3
import string
import random
import hashlib
from secrets import token_bytes



# CONSTANTS

PORT = 222
ROOT_FOLDER = '.'
DATABASE = 'database.db'
TOKEN_KEY = token_bytes(16)
TOKEN_SALT = token_bytes(8)



# INITIAL CONFIGURATION

chdir(ROOT_FOLDER)

db = sqlite3.connect(DATABASE).cursor()
db.execute('''
    CREATE TABLE IF NOT EXISTS users (
       id           INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
       login        VARCHAR(20) UNIQUE                NOT NULL,
       password     TEXT,
       salt         VARCHAR(5)
    );
''')



# RECEIVING/SENDING DATA FUNCTIONS

def get_data(conn, len):
	return conn.recv(len)

def get_int(conn, len=1):
	data = get_data(conn, len)
	result = 0
	print(data)
	for byte in data:
		print(byte)
		result << 8
		result += byte
	return result

def get_fixed_len_string(conn, len):
	return get_data(conn, len).decode('ascii')

def get_var_len_string(conn, length_of_lenght = 1):
	return get_fixed_len_string(conn, get_int(conn, length_of_lenght))

def return_error(conn, error_code):
	conn.send(bytes([error_code]))
	conn.close()



# MAIN FUNCTION

def handle_client(conn, addr):
	id = get_int(conn);

	if False: # for alligning conditions below
		pass
	elif (id == 0x01): # register
		login = get_fixed_len_string(conn, 20)
		password = get_var_len_string(conn)

	elif (id == 0x02): # login
		login = get_fixed_len_string(conn, 20)
		password = get_var_len_string(conn)

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

	print('Server started!')
	print('Waiting for clients...')

	while True:
		conn, addr = s.accept()	 # Establish connection with client.
		print('Got connection from', addr)
		p = Process(target = handle_client, args = (conn, addr))
		p.start()
		#p.join()
		pass

	s.close()
