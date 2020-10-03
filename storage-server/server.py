from socket import socket
from multiprocessing import Process
from os import system

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


# MAIN FUNCTION

def handle_ns_request(conn):
	log('Got connection from Name Server')
	id = get_int(conn)

	if False: # for alligning conditions below
		pass
	elif (id == 0x00): # Give the file to user 
		token = get_data(conn, 16)
		file_path = str(get_data(conn, get_int(conn)))

		log("Got 'give file' request from Name server: " + hex(token)[2::] + ", " + file_path)

	elif (id == 0x01): # Get the file from user
		token = get_data(conn, 16)
		file_size = get_int(4)
		file_path = get_data(conn, get_int(conn))

		log("Got 'get file' request from Name server: " + hex(token)[2::] + ", " + file_path + ", " str(file_size))

	elif (id == 0x02): # Get the file from server
		token = get_data(conn, 16)
		server_ip = get_int(4)
		file_path = get_data(conn, get_int(conn))

		log("Got 'get server file' request from Name server: " + hex(token)[2::] + ", " + file_path + ", " str(server_ip))

	elif (id == 0x03): # Eval of the given command
		command = str(get_data(conn, get_int(conn)))

		log("Got command from Name server: " + command)
		res = os.system(str(command))
		log("Result: " + command)

	else: # unknown id
		pass # TODO: return error

	# stdout.flush()



# START ACCEPTING CONNECTIONS

if __name__ == '__main__':

	send_i_was_born()

	s = socket()
	s.bind(('', PORT))
	s.listen()

	while True:
		conn, addr = s.accept()	 # Establish connection with client.
		p = Process(target = handle_client, args = (conn, addr))
		p.start()
		#p.join()
		pass

	s.close()