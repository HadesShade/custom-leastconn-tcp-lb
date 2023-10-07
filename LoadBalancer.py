#!/usr/bin/python3

#Main script of this custom TCP Load Balancer
import socket,sys,select,random,os,select,operator
from Logger import *


#This function will read provided configuration file and map the IP Addresses and Ports of the backend servers. Return the mapping result as dictionary.
#The configuration file will contains IP Addresses and Ports of every backend servers. One pair of IP_Address:Port per line.
#For configuration file example, see custom-lb.conf.example
def openConfigurationFile():
	listServers = dict()
	file = open("/etc/custom-lb.conf","r")
	servers = file.read().splitlines()
	for server in servers:
		ip = str(server.split(':')[0])
		port = int(server.split(':')[1])
		listServers[ip] = port
	LogInfo(f"Backend Servers Loaded : {listServers}")
	return listServers

#This function will initialize all connection/flow count to the backend server. Every count will start from 0 when initialized.
def initializeFlowsCount():
	fCounts = dict()
	listServers = openConfigurationFile()
	for ip in listServers:
		fCounts[ip] = 0
	LogInfo(f"Initialize Connection Count for Every Backend Server : {fCounts}")
	return fCounts

#Initialize all needed global variables
flowsCount = initializeFlowsCount()
flowsPair = dict()
all_sockets = list()
bind_pair = ('0.0.0.0', 80)

#This routine will initiate all needed variables for leastConn algorithm that will used as load balancing algorithm in this script.
#The flowsCount is ordered by the values in ascending sequence. Saved to sortedCount.
#The sortedCount will be separated to 3 lists. connectionCount will save all item values of sortedCount (Connection count to each Backend Servers)
#The choosenServers will save all item keys of sortedCount (Backend Server IP Address)
#The choosenPorts will save all port numbers of Backend Servers
def leastConnInitiate():
	connectionCount = list()
	choosenServers = list()
	choosenPorts = list()
	sortedCount = dict(sorted(flowsCount.items(), key=operator.itemgetter(1)))
	for server in sortedCount:
		connectionCount.append(int(sortedCount[server]))
		choosenServers.append(str(server))
		choosenPorts.append(int(openConfigurationFile()[server]))

	LogInfo(f"Current Connection Count for Every Backend Server : {flowsCount}")
	return connectionCount, choosenServers, choosenPorts

#This routine will start the load balancer.Processes are:
#1. Bind the load balancer to bind_pair defined in global variable (IP Address and port pair)
#2. Append the listen socket to all_sockets array defined in global variable
#3. Map the socket_readlist, socket_writelist, and socket_exception list to all_sockets array, null array, and null array respectively
#4. Call onConnectionAccept function when a connection request coming to the load balancer
#5. If the connection already established, receive data sent from client and pass to onDataReceive function
def startLB():
	listenSocket = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
	listenSocket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
	listenSocket.bind(bind_pair)
	listenSocket.listen()
	all_sockets.append(listenSocket)

	while True:
		socket_readlist, socket_writelist, socket_exceptionlist = select.select(all_sockets, [], [])
		for sock in socket_readlist:
			if sock == listenSocket:
				onConnectionAccept(listenSocket)
				break

			else:
				data = sock.recv(4096)
				if data:
					onDataReceive(sock, data)
				else:
					onConnectionClose(sock)
					break

#This routine will process incoming connection request to the load balancer. Processes are:
#1. Accept the connection and then map
#2. Initiate the leastConn algorithm by calling leastConnInitiate function
#3. Iterate the connection counts for each server. The connection counts returned from leastConnInitiate() will always in ascending order so it will iterate from the lowest connection count
#4. In the first iteration(lowest count of connection), it will try to create connection from client to the respective backend server.
#5. If the connection couldn't be created, It will iterate to the next item in the ascending sorted list of connection counts.
#6. Keep iterating. If all connection attempts failed to listed backend servers, it will return "No connection could be created to backend servers"
def onConnectionAccept(listenSocket):
	client_socket, client_ip = listenSocket.accept()
	server_connections, server_ips, server_ports = leastConnInitiate()
	server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

	for i in range (len(server_connections)):
		if i == len(server_connections) - 1:
			try:
				server_socket.connect((server_ips[i],server_ports[i]))
				all_sockets.append(client_socket)
				all_sockets.append(server_socket)
				flowsPair[client_socket] = server_socket
				flowsPair[server_socket] = client_socket
				flowsCount[server_ips[i]] += 1
				LogInfo(f"Established Connection from {client_socket.getpeername()[0]} to {server_ips[i]}")
				break
			except Exception:
				LogError("No Connection to Backend Servers can be Established")
				client_socket.close()
		else:
			try:
				server_socket.connect((server_ips[i],server_ports[i]))
				all_sockets.append(client_socket)
				all_sockets.append(server_socket)
				flowsPair[client_socket] = server_socket
				flowsPair[server_socket] = client_socket
				flowsCount[server_ips[i]] += 1
				LogInfo(f"Established Connection from {client_socket.getpeername()[0]} to {server_socket.getpeername()[0]}")
				break
			except Exception:
				LogError(f"Connection to {server_ips[i]} failed. Trying next server ...")
				continue

	LogInfo(f"Current Count for Every Backend Server : {flowsCount}")


#When connection established, send data from client to the respective backend server.
def onDataReceive(client_socket, data):
	flowsPair[client_socket].send(data)

#When connection terminated/closed from client to the backend server, remove the socket_pair mapping, close the socket connection, and re-count the connection count to the backend server.
def onConnectionClose(socket):
	socket_pair = flowsPair[socket]
	pair_ip1 = socket.getpeername()[0]
	pair_ip2 = socket_pair.getpeername()[0]
	all_sockets.remove(socket)
	all_sockets.remove(socket_pair)

	socket.close()
	socket_pair.close()

	del flowsPair[socket]
	del flowsPair[socket_pair]

	if pair_ip1 in flowsCount:
		flowsCount[pair_ip1] -=1

	elif pair_ip2 in flowsCount:
		flowsCount[pair_ip2] -=1

	else:
		pass

	LogInfo(f"Connection from {pair_ip1} to {pair_ip2} terminated gracefully.")
	LogInfo(f"Current Count for Every Backend Server : {flowsCount}")


#Main function.
if __name__ == "__main__":
	try:
		if sys.argv[1] == "start":
			startLB()
		elif sys.argv[1] == "stop":
			exit()
		else:
			print("Usage : LoadBalancer.py <start|stop>")

	except NameError:
		print("Usage : LoadBalancer.py <start|stop>")

	except IndexError:
		print("Usage : LoadBalancer.py <start|stop>")
