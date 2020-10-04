from socket import socket
from multiprocessing import Process
from os import system
import os
from pathlib import Path

# Parse config
def parse_config(filemae = 'config'):

	result = dict()

	with open(filename, "r") as f:
		for line in f:
			key, value = line.split('=')
			result[key] = value 

	return result

_d = parse_config()
PORT = int(_d['PORT'])
MASTER_IP = _d['MASTER_IP']
MASTER_PORT = _d['MASTER_PORT']



# HELPER FUNCTIONS
def log(string):
	print("%06d | %s" % (getpid(), string))
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

def send_i_was_born():
	conn = socket.socket((MASTER_IP, MASTER_PORT))
	conn.send(b'\x00' + int_to_bytes(PORT))
	data = conn.recv(1)
	if data != b'\x00':
		exit(0)
	conn.close()

def send_report(conn, operation, entity, string):
	conn.send(b'\x01')
	conn.send(b'\x00' if operation == 'deleted' else '\x01')
	conn.send(b'\x00' if entity == 'file' else '\x01')
	conn.send(int_to_bytes(len(string)))
	conn.send(string.encode('utf-8'))
	conn.close()

def send_response(conn, status_code):
	conn.send(int_to_bytes(status_code))
	conn.close()


todo_list = dict()

def get_file_from_server(server_ip, server_port, token, file_path):
	conn = socket.socket((int_to_ip(server_ip), server_port))
	conn.send(token)
	status_code, file_size = get_int(conn, 1), get_int(conn, 4)

	f = open(file_path, "wb")

	block_size = 1024
	for i in range(0, file_size - block_size, block_size):
		data = conn.recv(block_size)
		f.write(data)

	data = conn.recv(block_size - file_size % block_size)
	f.write(data)

	f.close()
	conn.close()


# MAIN FUNCTION
def handle_ns_request(conn):
	log('Got connection from Name Server')
	id = get_int(conn)

	if False: # for alligning conditions below
		pass
	elif (id == 0x00): # Send the file to user 
		token = get_data(conn, 16)
		file_path = get_data(conn, get_int(conn)).decode('utf-8')

		log("Got 'send file' request from Name server: " + hex(token)[2::] + ", " + file_path)

		if (os.isfile(file_path)):
			file_size = Path(file_path).stat().st_size
			todo_list[token] = ('send', file_size, file_path)
			sock.send(b"\x00")
		else:
			sock.send(b"\x01")

	elif (id == 0x01): # Get the file from user
		token = get_data(conn, 16)
		file_size = get_int(4)
		file_path = get_data(conn, get_int(conn)).decode('utf-8')

		log("Got 'get file' request from Name server: " + hex(token)[2::] + ", " + file_path + ", " str(file_size))

		todo_list[token] = ('get', file_size, file_path)
		sock.send(b"\x00")

	elif (id == 0x02): # Get the file from server
		token = get_data(conn, 16)
		server_ip = get_int(4)
		server_port = get_int(2)
		file_path = get_data(conn, get_int(conn)).decode('utf-8')

		log("Got 'get server file' request from Name server: " + hex(token)[2::] + ", " + file_path + ", " str(server_ip))

		p = Process(target = get_file_from_server, args = (server_ip, server_port, token, file_path))
		p.start()
		sock.send(b"\x00")

	elif (id == 0x03): # Eval of the given command
		command = str(get_data(conn, get_int(conn)))

		log("Got command from Name server: " + command)
		res = os.system(str(command))
		log("Result: " + command)

	else: # unknown id
		sock.send(b"\x02")
		pass # TODO: return error

	conn.close()


def handle_client_request(conn, addr):
	log('Got connection from Client ' + addr)
	token = get_data(conn, 16)

	if token not in todo_list:
		conn.send(b"\x01")
		return

	action, file_size, file_path = todo_list[token]

	block_size = 1024

	if action == "send":
		conn.send(b'\x00' + int_to_bytes(file_size))

		f = open(file_path, 'rb')

		for i in range(0, file_size, block_size):
			data = f.read(block_size)
			conn.send(data)

		f.close()
	elif action == "get":
		f = open(file_path, 'wb')

		for i in range(0, file_size - block_size, block_size):
			data = conn.recv(block_size)
			f.write(data)

		data = conn.recv(block_size - file_size % block_size)
		f.write(data)
		
		f.close()

	conn.close()	



# START ACCEPTING CONNECTIONS
if __name__ == '__main__':

	send_i_was_born()

	s = socket()
	s.bind(('', PORT))
	s.listen()

	while True:
		conn, addr = s.accept()	 # Establish connection with client.
		
		if addr == MASTER_IP:
			p = Process(target = handle_ns_request, args = (conn))
			p.start()
		else:
			p = Process(target= handle_client_request, args = (conn, addr))
			p.start()

	s.close()