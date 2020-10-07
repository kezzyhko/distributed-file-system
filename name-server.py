from socket import socket, timeout as socket_error
from threading import Thread, get_ident
import sys
from os import system
import sqlite3
from hashlib import pbkdf2_hmac as password_hash
from secrets import token_bytes, token_hex
from time import sleep
from ipaddress import ip_address



# CONSTANTS

PORT = 1234
ROOT_FOLDER = '/files'
DATABASE = 'database.db'
PING_DELAY = 60
PING_TIMEOUT = 5
STORAGE_SERVER_MEMORY = 2 ** (8 * 4) # 4GB
ALLOW_LESS_REPLICAS = True



# INITIAL DATABASE CONFIGURATION

db_conn = sqlite3.connect(
	DATABASE,
	check_same_thread = False
) 
db_cursor = db_conn.cursor()

db_cursor.execute('PRAGMA foreign_keys = ON;')

db_cursor.execute('''
    CREATE TABLE IF NOT EXISTS users (
        id           INTEGER PRIMARY KEY AUTOINCREMENT  NOT NULL,
        login        VARCHAR(20)  UNIQUE                NOT NULL,
        password     BLOB(16)                           NOT NULL,
        salt         BLOB(5)                            NOT NULL
    );
''')

db_cursor.execute('''
    CREATE TABLE IF NOT EXISTS tokens (
        id           INTEGER PRIMARY KEY AUTOINCREMENT  NOT NULL,
        login        VARCHAR(20)                        NOT NULL,
        token        BLOB(32)     UNIQUE                NOT NULL,
        FOREIGN KEY (login) REFERENCES users (login)
    );
''')

db_cursor.execute('''
    CREATE TABLE IF NOT EXISTS file_structure (
        id           INTEGER PRIMARY KEY AUTOINCREMENT  NOT NULL,
        login        VARCHAR(20)                        NOT NULL,
        path         VARCHAR(256)                       NOT NULL,
        size         INTEGER(4),
        created_on   DATETIME DEFAULT CURRENT_TIMESTAMP NOT NULL,
        FOREIGN KEY (login) REFERENCES users (login),
        UNIQUE (path, size)
    );
''')

db_cursor.execute('''
    CREATE TABLE IF NOT EXISTS servers (
        id           INTEGER PRIMARY KEY AUTOINCREMENT  NOT NULL,
        ip           INTEGER(4)                         NOT NULL,
        port         INTEGER(2)                         NOT NULL
    );
''')

db_cursor.execute('''
    CREATE TABLE IF NOT EXISTS files_on_servers (
        id           INTEGER PRIMARY KEY AUTOINCREMENT  NOT NULL,
        file_id      INTEGER                            NOT NULL,
        server_id    INTEGER                            NOT NULL,
        FOREIGN KEY (file_id)   REFERENCES file_structure (id) ON DELETE CASCADE,
        FOREIGN KEY (server_id) REFERENCES servers (id)        ON DELETE CASCADE
    );
''')

db_conn.commit()



# HELPER FUNCTIONS

def log(string):
	print("%06d | %s" % (get_ident(), string))
	sys.stdout.flush()

def get_function_by_addr(addr):
	if addr[0] == '25.108.175.17':
		return handle_storage_server
	else:
		return handle_client

def foreach_storage_server(func, additional_params=(), delays=False, servers=None):
	storage_servers_list_copy = storage_servers_list.copy() if (servers == None) else servers.copy()
	errors = set()

	for server in storage_servers_list_copy:
		res = func(server, *additional_params)
		if not res:
			storage_servers_list.remove(server)
			errors.add(server)
			if delays: sleep(PING_DELAY / len(storage_servers_list_copy))
	if delays: sleep(PING_DELAY / len(storage_servers_list_copy))

	if len(errors) > 0:
		params = ()
		for server in errors:
			params += (int(ip_address(server[0])), server[1])
		db_cursor.execute('''
			SELECT login, path, size
			FROM servers AS s, files_on_servers AS fs, file_structure AS f
			WHERE fs.file_id = f.id AND fs.server_id = s.id
			AND (ip, port) IN (VALUES ''' + (', '.join(['(?, ?)']*len(errors))) + ''')
		''', params)
		for server in errors:
			db_cursor.execute('DELETE FROM servers WHERE ip = ? AND port = ?;', (int(ip_address(server[0])), server[1]))
			db_conn.commit() # TODO: check if ok?
		for file in db_cursor.fetchall():
			file_copy_or_replicate(*file, 1)

	return errors

def get_path_on_storage_server(login, path=''):
	return '/'.join(filter(None, [ROOT_FOLDER, login, path]))

def is_valid_filename(string):
	# check special conditions
	if	(string == '' or
		string[0] == ' ' or
		string[-1] == ' ' or
		string[-1] == '.'):
			return False
	
	# check each prohibited char
	for c in "\x00\\\"/:*<>|?\n\r":
		if c in string:
			return False

	# all good
	return True



# DATABASE FUNCTION

def servers_from_db_format(db_format_servers):
	servers = set()
	for row in db_format_servers:
		servers.add((str(ip_address(row[0])), row[1]))
	return servers

def get_servers_for_upload(conn, count = 2, filesize = 0, exclude_servers_params = ()):
	params = ()
	sql = '''
		SELECT ip, port, COALESCE(sum(size), 0) as mem
		FROM
			(servers as s LEFT JOIN files_on_servers as fs ON s.id = fs.server_id)
			LEFT JOIN file_structure as f ON f.id = fs.file_id
	'''
	if (exclude_servers_params != ()):
		sql += '''
			WHERE server_id NOT IN (
				SELECT server_id
				FROM servers as s, file_structure as f, files_on_servers as fs
				WHERE s.id = fs.server_id AND f.id = fs.file_id
				AND login = ? AND path = ?
			)
		'''
	sql += '''
		GROUP BY server_id
		ORDER BY mem ASC 
		LIMIT ?
	'''
	db_cursor.execute(sql, exclude_servers_params + (count,))
	servers = db_cursor.fetchall()

	# check 'not enough space' #TODO!!!
	errors = set()
	if conn != None:
		if (ALLOW_LESS_REPLICAS and len(servers) == 0) or (not ALLOW_LESS_REPLICAS and len(servers) != count):
			return_status(conn, 0x23)
		for server in servers:
			if STORAGE_SERVER_MEMORY - server[2] < filesize:
				if ALLOW_LESS_REPLICAS:
					errors.add(server)
				else:
					return_status(conn, 0x23)
	
	return servers_from_db_format(set(servers) - errors)

def get_servers_with_files(conn, login, path, count = None):
	sql = '''
		SELECT ip, port
		FROM servers as s, file_structure as f, files_on_servers as fs
		WHERE s.id = fs.server_id AND f.id = fs.file_id
		AND login = ? AND path = ?
	'''
	params = (login, path)
	if (count != None):
		sql += 'LIMIT ?'
		params += (count,)
	db_cursor.execute(sql, params)
	servers = db_cursor.fetchall()

	if conn != None:
		if (ALLOW_LESS_REPLICAS and len(servers) == 0) or (not ALLOW_LESS_REPLICAS and len(servers) != count):
			return_status(conn, 0x80)

	return servers_from_db_format(servers)



# RECEIVING DATA FUNCTIONS

def get_data(conn, length):
	return conn.recv(length)

def get_int(conn, length=1):
	data = get_data(conn, length)
	result = 0
	for byte in data:
		result <<= 8
		result |= byte
	return result

def get_fixed_len_string(conn, length):
	return get_data(conn, length).decode('utf-8').rstrip('\x00')

def get_var_len_string(conn, length_of_lenght = 1):
	return get_fixed_len_string(conn, get_int(conn, length_of_lenght))

def get_login(conn):
	token = get_data(conn, 32)
	db_cursor.execute("SELECT login FROM tokens WHERE token = ?", (token,))
	row = db_cursor.fetchone()
	if row == None:
		return_status(conn, 0x15)
	else:
		return row[0]



# RESPONSE FUNCTIONS

def return_status(conn, code, message=''):
	l = len(message)
	conn.send(bytes([code, l//256, l%256]))
	if (isinstance(message, str)):
		message = message.encode('utf-8')
	log('Returned status code %02x with message %s' % (code, message))
	conn.send(message)
	conn.close()
	sys.exit()

def return_token(conn, login):
	log('Successfull login/registration as "%s"' % login)
	token = token_bytes(32)
	db_cursor.execute("INSERT INTO tokens (login, token) VALUES (?, ?);", (login, sqlite3.Binary(token)))
	if db_cursor.rowcount == 1:
		db_conn.commit()
		conn.send(b'\x00')
		conn.send(token)
		conn.close()
		sys.exit()
	else:
		return_status(conn, 0x10) # Unknown auth error

def return_server(conn, ip, port, token):
	ip = ip_address(ip)
	log('Returned server %s:%d' % (str(ip), port))
	conn.send(b'\x00')
	conn.send(b'\x01' if ip.version == 4 else b'\x02')
	conn.send(bytes([port//256, port%256]))
	conn.send(int(ip).to_bytes(4 if ip.version == 4 else 16, 'big'))
	conn.send(token)
	conn.close()
	sys.exit()

def storage_server_response(conn, code):
	log('Returned status code %02x' % code)
	conn.send(bytes([code]))
	conn.close()



# STORAGE SERVER QUERIES

def server_send(server, data):
	log("Sending data to {}".format(server))
	try:
		conn = socket()
		conn.settimeout(PING_TIMEOUT)
		conn.connect(server)
		for d in data:
			log("Sending {}".format(d))
			conn.send(d)
			log("Sent {}".format(d))
		return (get_int(conn) == 0)
	except socket_error as e: 
		log('Could not connect to {}'.format(server))
		return False
	finally:
		conn.close()

def server_eval(server, cmd):
	l = len(cmd)
	return server_send(server, [b'\x03', bytes([l//256, l%256]), cmd.encode('utf-8')])

def server_ping(server):
	log('Send ping to {}'.format(server))
	return server_send(server, [b'\x04'])

def server_create_dir(server, login, path=''):
    return server_eval(server, 'Path("%s").mkdir(parents=True,exist_ok=True)' % get_path_on_storage_server(login, path))

def server_delete_dir(server, login, path=''):
	return server_eval(server, 'rmtree("%s")' % get_path_on_storage_server(login, path))

def server_create_file(server, login, path):
    return server_eval(server, 'Path("%s").touch(exist_ok=True)' % get_path_on_storage_server(login, path))

def server_delete_file(server, login, path):
	# TODO: remove from db?
	return server_eval(server, 'os.remove("%s")' % get_path_on_storage_server(login, path))

def server_move_files(server, login, source, destination):
	return server_eval(server, 'os.replace("%s", "%s")' % (
		get_path_on_storage_server(login, source),
		get_path_on_storage_server(login, destination),
	))

def server_initialize(server, login, new_user=False):
	if not new_user:
		res1 = server_delete_dir(server, login)
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

def file_copy_or_replicate(login, source, filesize, nodes_count, destination = None, conn = None):
	servers = get_servers_with_files(conn, login, source, count = nodes_count)
	destination_servers = get_servers_for_upload(
		conn,
		count = len(servers),
		filesize = filesize,
		exclude_servers_params = (login, source) if (destination == None) else () # replication
	)
	if (destination == None): # replication
		destination = source
	for server_pair in zip(servers, destination_servers):
		token = token_bytes(16)
		foreach_storage_server(
			server_send,
			(['\x00', token, bytes([len(source)]), source.encode('utf-8')],),
			servers = {server_pair[0]}
		)
		foreach_storage_server(
			server_send,
			([
				'\x02', token,
				int(ip_address(server_pair[0][0])).to_bytes(4, 'big'),
				bytes([server_pair[0][1]//256, server_pair[0][1]%256, len(destination)]),
				destination.encode('utf-8')
			],),
			servers = {server_pair[1]}
		)

def handle_client(conn, addr):
	log('Got connection from client {}'.format(addr))
	id = get_int(conn)

	if   (id == 0x00): # logout
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

	elif (id == 0x04 or id == 0x06 or id == 0x0C): # file create / file write / directory create
		action = 'file_create' if (id == 0x04) else ('file_write' if id == 0x06 else 'directory_create')
		login = get_login(conn)
		if (id == 0x06): # file write
			path_len = get_int(conn)
			size = get_int(conn, 4)
			path = get_fixed_len_string(conn, path_len)
		else:            # file/directory create
			size = None if (action == 'directory_create') else 0
			path = get_var_len_string(conn)
		folder, _, filename = path.rpartition('/')

		db_cursor.execute("SELECT size FROM file_structure WHERE login = ? AND path = ?;", (login, path))
		row = db_cursor.fetchone()
		if row != None:
			return_status(conn, 0x32 if row[0] == None else 0x22) # Directory/File already exists
		else:
			db_cursor.execute("SELECT path FROM file_structure WHERE size IS NULL AND login = ? AND path = ?;", (login, folder))
			if db_cursor.fetchone() != None:
				return_status(conn, 0x31) # Directory does not exist
			elif not is_valid_filename(filename):
				return_status(conn, 0x33 if (action == 'directory_create') else 0x24) # Prohibited directory/file name
			else:
				# finally, creating file/directory
				db_cursor.execute("INSERT INTO file_structure (login, path, size) VALUES (?, ?, ?);", (login, path, size))
				if db_cursor.rowcount == 1:
					db_conn.commit()
					if (action == 'directory_create'):
						foreach_storage_server(server_create_dir, (login, path))
						return_status(conn, 0x00)
					elif (action == 'file_create'):
						foreach_storage_server(server_create_file, (login, path), servers = get_servers_for_upload(conn, count = 2))
						return_status(conn, 0x00)
					elif (action == 'file_write'):
						token = token_bytes(16)
						servers = get_servers_for_upload(conn, count = 1, filesize = size)
						path_on_server = get_path_on_storage_server(login, path)
						foreach_storage_server(
							server_send,
							([b'\x01', token, int.to_bytes(size, 4, 'big'), bytes([len(path_on_server)]), path_on_server.encode('utf-8')],),
							servers = servers
						)
						# TODO: check other servers if this is unreachable?
						server = servers.pop()
						return_server(conn, server[0], server[1], token)
					else:
						return_status(conn, 0x80) # Unknown error
				else:
					return_status(conn, 0x30 if (action == 'directory_create') else 0x20) # Unknown directory/file error

	elif (id == 0x05 or id == 0x09 or id == 0x0A): # file read / file copy / file move
		login = get_login(conn)
		if (id == 0x05): # file read
			filepath = get_var_len_string(conn)
		else:
			source_len = get_int(conn)
			destination_len = get_int(conn)
			filepath = get_fixed_len_string(conn, source_len)
			destination = get_fixed_len_string(conn, destination_len)

		db_cursor.execute("SELECT size FROM file_structure WHERE login = ? AND path = ? AND size IS NOT NULL;", (login, filepath))
		row = db_cursor.fetchone()
		if row == None:
			return_status(conn, 0x21) # File does not exist
		else:
			if (id == 0x05): # file read
				servers = get_servers_with_files(conn, login, filepath, count = 1)
				server = servers.pop()
				token = token_bytes(16)
				errors = foreach_storage_server(
					server_send,
					(['\x00', token, bytes([len(filepath)]), filepath.encode('utf-8')],),
					servers = {server}
				)
				if (len(errors) != 0):
					return_status(0x80)
					# TODO: try to connect to other servers?
				else:
					return_server(conn, server[0], server[1], token)
			elif (id == 0x09): # file copy
				file_copy_or_replicate(login, filepath, row[0], 2, destination, conn)
				return_status(conn, 0x00) # OK
			elif (id == 0x0A): # file move
				servers = get_servers_with_files(conn, login, filepath, count = None)
				foreach_storage_server(server_move_files, (login, source, destination), servers = servers)
				return_status(conn, 0x00) # OK
			else:
				return_status(conn, 0x80) # Unknown error

	#     id == 0x06   # look above

	elif (id == 0x07 or id == 0x0D): # file/directory delete
		login = get_login(conn)
		path = get_var_len_string(conn)
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
					foreach_storage_server(server_delete_dir, (login, path))
				else:
					servers = get_servers_with_files(None, login, path)
					foreach_storage_server(server_delete_file, servers = servers)
					
				return_status(conn, 0x00)
			else:
				return_status(conn, 0x30 if deleting_dir else 0x20) # Unknown directory/file error


	elif (id == 0x08): # file info
		login = get_login(conn)
		filepath = get_var_len_string(conn)
		_, _, filename = filepath.rpartition('/')

		db_cursor.execute('''
			SELECT size, count(server_id), created_on
			FROM file_structure as f LEFT JOIN files_on_servers as fs ON fs.file_id = f.id
			WHERE size IS NOT NULL AND login = ? AND path = ?;
		''', (login, filepath))
		row = db_cursor.fetchone()
		if row == None:
			return_status(conn, 0x21) # File does not exist
		else:
			return_status(conn, 0x00, ("%d %d %s " % row) + filename)

	#     id == 0x09   # look above
	#     id == 0x0A   # look above

	elif (id == 0x0B): # directory read
		login = get_login(conn)
		dirname = get_var_len_string(conn)

		db_cursor.execute("SELECT size FROM file_structure WHERE size IS NULL AND login = ? AND path = ?;", (login, path))
		row = db_cursor.fetchone()
		if row == None:
			return_status(conn, 0x31) # Directory does not exist
		else:
			db_cursor.execute("SELECT path, size FROM file_structure WHERE login = ? AND path REGEXP ?;", (login, '^%s\\/[^\\/]*$' % filepath))
			directory_list = db_cursor.fetchall()

			msg = ("total %d\r\n" % len(directory_list))
			for entity in directory_list:
				msg += ("%c %s\r\n" % ('d' if entity[1] == None else 'f', entity[0]))

			return_status(conn, 0x00, msg)

	#     id == 0x0C   # look above
	#     id == 0x0D   # look above

	else: # unknown id
		return_status(conn, 0x81)

	log('Thread end')



# ADD STORAGE SERVERS FROM DB

storage_servers_list = set()

db_cursor.execute('SELECT ip, port FROM servers;')
for server in db_cursor.fetchall():
	storage_servers_list.add((str(ip_address(server[0])), server[1]))



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

ping_thread = Thread(target = ping_storage_servers)
ping_thread.start()
#ping_thread.join()



# HANDLE STORAGE SERVER

def handle_storage_server(conn, addr):
	log('Got connection from storage server {}'.format(addr))
	id = get_int(conn)

	if False: # for aligning conditions below
		pass

	elif (id == 0x00): # new storage server
		port = get_int(conn, 2)
		server = (addr[0], port)
		storage_servers_list.add(server)
		db_cursor.execute("INSERT INTO servers (ip, port) VALUES (?, ?)", (int(ip_address(addr[0])), port))
		if db_cursor.rowcount == 1:
			db_conn.commit()
			storage_server_response(conn, 0x00) # OK

			# send full folder structure to the new server
			db_cursor.execute("SELECT login, path FROM file_structure WHERE size IS NULL")
			folders_string = '/files'
			for row in db_cursor.fetchall():
				folders_string += '\n'
				folders_string += get_path_on_storage_server(*row)
			server_delete_dir(server, '')
			cmd = '[Path(x).mkdir(parents=True,exist_ok=True) for x in """{}""".split("\\n")]'.format(folders_string)
			foreach_storage_server(server_eval, (cmd,), servers = {server})
		else:
			storage_server_response(conn, 0x80) # Unknown server error

	elif (id == 0x01): # report
		operation = get_int(conn)
		entity_type = get_int(conn)
		path_on_server = get_var_len_string(conn)
		login, _, path = path_on_server.lpartition('/')
		ip = int(ip_addr(addr[0]))

		if   operation == 0x00: # deleted
			db_cursor.execute('''
				DELETE FROM files_on_servers WHERE id IN (
					SELECT fs.id
					FROM files_on_servers as fs, servers AS s, file_structure AS f
					WHERE fs.server_id = s.id AND fs.file_id = f.id
					AND login = ? AND path = ? AND ip = ?
				);
			''', (login, path, ip))
			if db_cursor.rowcount == 1:
				db_conn.commit()
				storage_server_response(conn, 0x00) # OK
			else:
				storage_server_response(conn, 0x80) # Unknown server error

		elif operation == 0x01: # created
			db_cursor.execute('''
				INSERT INTO files_on_servers (server_id, file_id) VALUES (
					(SELECT id FROM servers WHERE ip = ?),
					(SELECT id FROM file_structure WHERE login = ? AND path = ?)
				);
			''', (ip, login, path))
			if db_cursor.rowcount == 1:
				db_conn.commit()
				storage_server_response(conn, 0x00) # OK
			else:
				storage_server_response(conn, 0x80) # Unknown server error

		if entity_type == 0x00: # file, not directory
			# replicate file
			db.execute('''
				SELECT size, count(server_id)
				FROM files_on_servers as fs, servers AS s, file_structure AS f
				WHERE fs.server_id = s.id AND fs.file_id = f.id
				AND login = ? AND path = ?
			''', (login, path))
			row = db_cursor.fetchone()
			if row == None:
				storage_server_response(conn, 0x00) # OK
				# TODO: if not ALLOW_LESS_REPLICAS: give some error to user?
			elif row[1] < 2:
				file_copy_or_replicate(login, path, row[0], 1)

		else:
			storage_server_response(conn, 0x81) # Wrong request id

	else: # unknown id
		storage_server_response(conn, 0x81)

	log('Thread end')



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
