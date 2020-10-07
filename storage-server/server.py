from socket import socket
from threading import Thread, get_ident
from os import system
import os
from pathlib import Path
from sys import stdout
import time
from time import strftime
from time import gmtime
from shutil import rmtree

# Parse config
def parse_config(filename = 'config'):

	result = dict()

	with open(filename, "r") as f:
		for line in f:
			key, value = line.replace('\n', '').split('=')
			result[key] = value 

	return result

_d = parse_config()
PORT = int(_d['PORT'])
MASTER_IP = _d['MASTER_IP']
MASTER_PORT = int(_d['MASTER_PORT'])


# HELPER FUNCTIONS
def log(string):
	print("%s: %06d | %s" % (strftime("%H:%M:%S", gmtime(time.time())), get_ident(), string))
	stdout.flush()

def int_to_bytes(n):
	res = b''
	while n > 0:
		n, b = divmod(n, 256)
		res = bytes([b]) + res

	return res

def int_to_ip(n):
	n, r = divmod(n, 256)
	res = '.' + str(r)
	n, r = divmod(n, 256)
	res = '.' + str(r) + res
	n, r = divmod(n, 256)
	res = '.' + str(r) + res
	return str(n) + res

# RECEIVING DATA FUNCTIONS
def get_data(conn, length):
	return conn.recv(length)

def get_int(conn, length=1):
	data = get_data(conn, length)

	#log(data)

	result = 0
	for byte in data:
		result <<= 8
		result |= byte
	return result

def get_fixed_len_string(conn, length):
	return get_data(conn, length).decode('ascii').rstrip('\x00')

def get_var_len_string(conn, length_of_lenght = 1):
	return get_fixed_len_string(conn, get_int(conn, length_of_lenght))



# SENDING DATA FUNCTIONS

def send_i_was_born():
	conn = socket()
	conn.connect((MASTER_IP, MASTER_PORT))

	conn.send(b'\x00' + int_to_bytes(PORT))
	data = conn.recv(1)
	if data != b'\x00':
		exit(0)
	conn.close()

	log("Sent 'I was born' to the name server")

def send_report_to_name_server(operation, entity, string):
	conn = socket()
	conn.connect((MASTER_IP, MASTER_PORT))

	conn.send(b'\x01')
	conn.send(b'\x00' if operation == 'deleted' else b'\x01')
	conn.send(b'\x00' if entity == 'file' else b'\x01')
	
	data = string[7:].encode('utf-8')
	conn.send(int_to_bytes(len(data)))
	conn.send(data)
	conn.close()

def send_response(conn, status_code):
	conn.send(int_to_bytes(status_code))
	conn.close()


todo_list = dict()

def get_file_from_server(server_ip, server_port, token, file_path):
	conn = socket()
	conn.connect((int_to_ip(server_ip), server_port))

	conn.send(token)
	status_code, file_size = get_int(conn, 1), get_int(conn, 4)

	f = open(file_path, "wb")

	block_size = 1024
	for i in range(0, file_size, block_size):
		data = conn.recv(block_size)
		f.write(data)

	data = conn.recv(file_size % block_size)
	f.write(data)

	f.close()
	conn.close()


# MAIN FUNCTION
def handle_ns_request(conn, addr):
	log('Got connection from Name Server')
	id = get_int(conn)

	if (id == 0x00): # Send the file to user 
		token = get_data(conn, 16)
		file_path = get_data(conn, get_int(conn)).decode('utf-8')

		log("Got 'send file' request from Name server: " + hex(int.from_bytes(token, "big"))[2::] + ", " + file_path)

		if (Path(file_path).is_file()):
			file_size = Path(file_path).stat().st_size
			todo_list[token] = ('send', file_size, file_path)
			conn.send(b"\x00")
		else:
			conn.send(b"\x01")


	elif (id == 0x01): # Get the file from user
		token = get_data(conn, 16)

		file_size = get_int(conn, 4)

		path_len = get_int(conn, 1)

		file_path = get_data(conn, path_len).decode("UTF-8")

		log("Got 'get file' request from Name server: " + hex(int.from_bytes(token, "big"))[2::] + ", " + file_path + ", " + str(file_size))

		todo_list[token] = ('get', file_size, file_path)
		conn.send(b"\x00")

	elif (id == 0x02): # Get the file from server
		token = get_data(conn, 16)
		server_ip = get_int(conn, 4)
		server_port = get_int(conn, 2)

		path_len = get_int(conn, 1)

		file_path = get_data(conn, path_len).decode('utf-8')

		log("Got 'get server file' request from Name server: " + hex(int.from_bytes(token, "big"))[2::] + ", " + file_path + ", " + str(server_ip))

		p = Thread(target = get_file_from_server, args = (server_ip, server_port, token, file_path))
		p.start()
		conn.send(b"\x00")

	elif (id == 0x03): # Eval of the given command
		command = get_data(conn, get_int(conn, 2)).decode("utf-8")

		log("Got command from Name server: " + command)
		try:
			exec(str(command))
			log("Result: OK")
			conn.send(b'\x00')
		except Exception as ex:
			log("Result: NOT ok, " + str(ex))
			conn.send(b'\x01')

	elif (id == 0x04): # Ping-pong
		conn.send(b"\x00")
		log("Got 'ping' from Name server")

	else: # unknown id
		conn.send(b"\x02")
		pass # TODO: return error

	conn.close()


def handle_client_request(conn, addr):
	log('Got connection from Client ' + addr[0])
	token = get_data(conn, 16)

	if token not in todo_list:
		conn.send(b"\x01")
		return

	action, file_size, file_path = todo_list[token]

	block_size = 1024

	if action == "send":
		try:
			conn.send(b'\x00' + int_to_bytes(file_size))

			f = open(file_path, 'rb')

			for i in range(0, file_size, block_size):
				data = f.read(block_size)
				conn.send(data)

			f.close()

			log('Send file ' + file_path + ' to Client ' + addr[0])
		except Exception as ex:
			log(ex)
	elif action == "get":
		try:
			f = open(file_path, 'wb')

			i = 0
			while i < file_size:
				data = conn.recv(block_size if block_size < (file_size - i) else (file_size - i))
				i += len(data)
				f.write(data)
			
			f.close()

			log('Got file ' + file_path + ' from Client ' + addr[0])

			send_report_to_name_server('created', 'file', file_path)
		except Exception as ex:
			log(ex)
	conn.close()	


# START ACCEPTING CONNECTIONS
if __name__ == '__main__':
	send_i_was_born()

	s = socket()
	s.bind(('', PORT))
	s.listen()

	while True:
		conn, addr = s.accept()	 # Establish connection with client.

		if addr[0] == MASTER_IP:
			p = Thread(target = handle_ns_request, args = (conn, addr))
			p.start()
		else:
			p = Thread(target= handle_client_request, args = (conn, addr))
			p.start()

	s.close()