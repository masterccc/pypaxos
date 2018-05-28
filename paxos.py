#!/usr/bin/env python3

# Run : ./paxos.py ip

from random import randint
from threading import Thread
import socket
import sys

PEERS_PORT = 5555
CLIENTS_PORT = 6666

BROADCAST_IP = "172.17.255.255"
LOCAL_IP = sys.argv[1]

connected_hosts = []
connected_socks = []

PTC_PRESENT = "!p4x0s_present"
PTC_CONNECT = "!p4x0s_connection"

PTC_CLI_ASK = "!p4x0s_cli_ask"
PTC_CLI_RELEASE_SRV = "!p4x0s_release"

PTC_PROCESS = "!p4x0s_processing"

PTC_ASK_UPDATE = "!p4x0s_askupdate"
PTC_SEND_UPDATE = "!p4x0s_sendupdate"

PTC_ASK_PROMISE = "!p4x0s_doyoupromise"
PTC_PROMISE = "!p4x0s_i_promise"
PTC_IDONTPROMISE = "!p4x0s_i_dont"

PTC_COMMIT = "!p4x0s_commit" 
PTC_ACK = "!p4x0s_ack"

PTC_STOP = "!p4x0s_stop"

verrou = 0
number = 0
leClient = None


def print_state(number,verrou):
	print("======== New State =========")
	print("Number = " + str(number))
	print("lock = " + str(verrou))
	print("============================")


def peerRequestManager(request, asker):
	
	global verrou
	global number
	global leClient

	tab_cmd = request.split(" ")

	# PROMISE LOCKNUMBER
	if(tab_cmd[0] == PTC_ASK_PROMISE):
		
		locknumber = int(tab_cmd[1])
		if(locknumber <= verrou):
			print("[SND TO PEER] >>> " + PTC_IDONTPROMISE)
			asker.send(PTC_IDONTPROMISE.encode())
		else:
			print("[SND TO PEER] >>> " + PTC_PROMISE)
			asker.send(PTC_PROMISE.encode())
			verrou = locknumber

	if(tab_cmd[0] == PTC_IDONTPROMISE):
		print("[RCV idontpromise]")
		leClient.send("Out of order, retry later".encode())
		print("[SND needUpdate]")
		connected_socks[0].send(PTC_ASK_UPDATE.encode())


	if(tab_cmd[0] == PTC_ASK_UPDATE):
		print("RCV need update ")
		resp = PTC_SEND_UPDATE + " " + str(number) + " " + str(verrou)
		print("SND " + resp)
		asker.send(resp.encode())

	if(tab_cmd[0] == PTC_SEND_UPDATE):
		n = tab_cmd[1]
		lo = tab_cmd[2]
		print("received update " + n + lo)
		number = int(n)
		verrou = int(lo)
		print_state(number, verrou)

	if(tab_cmd[0] == PTC_PROMISE):
		leClient.send(PTC_PROCESS.encode())
		commit = PTC_COMMIT + " " + number
		print("[SND TO PEER] >>> " + commit)
		asker.send(commit.encode())

	if(tab_cmd[0] == PTC_ACK):
		print("Saved value by another node")

	if(tab_cmd[0] == PTC_COMMIT):
		value = int(tab_cmd[1])
		number = value
		print("[SND TO PEER] >>> " + PTC_ACK)
		print_state(number, verrou)
		asker.send(PTC_ACK.encode())


# Read data from TCP connection
class PeerTCPReceiver(Thread):
	 def __init__(self, sock):
	 	Thread.__init__(self)
	 	self.sock = sock

	 def run(self):
	 	while(True):
	 		data = self.sock.recv(1024)
	 		data = data.decode('utf-8')
	 		if(data == ""):
	 			print("Peer leaves")
	 			print(str(connected_hosts))
	 			print(str(connected_socks))
	 			print("doit retirer :" + str(self.sock.getpeername()[0]))
	 			connected_hosts.remove(self.sock.getpeername()[0])
	 			connected_socks.remove(self.sock)
	 			break
	 		print("[RCV from PEER] <<< " + data)
	 		peerRequestManager(data, self.sock)


# Read data from other servers
class PeersTCPConnectionCollector(Thread):

	def __init__(self,addr):
		Thread.__init__(self)
		self.peers = []
		self.sock = socket.socket()
		self.sock.bind((addr,PEERS_PORT))
		self.sock.listen(1)

	def run(self):
		print("Listening for peers on TCP:"+str(PEERS_PORT)+" ...")
		while(True):
			conn, client = self.sock.accept()
			self.peers.append((conn, client))
			connected_hosts.append(client[0])
			connected_socks.append(conn)
			list2 = PeerTCPReceiver(conn)
			list2.start()
			print("[RCV] TCP connection request from ", str(conn))


class PeersUDPConnectionCollector(Thread):

	def __init__(self):
		Thread.__init__(self)
		self.addr = LOCAL_IP
		self.rSock = socket.socket()
		self.connected = []

	def run(self):

		signaled = False
		call = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
		print("Listen on UDP:" + str(PEERS_PORT) + " (threaded)")
		call.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
		call.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
		call.bind((BROADCAST_IP,PEERS_PORT))
		

		while(True):

			if(not signaled):
				print("Send Broadcast on UDP:" + str(PEERS_PORT))
				mcall = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
				mcall.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
				mcall.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
				mcall.sendto(PTC_CONNECT.encode(), (BROADCAST_IP, PEERS_PORT))
				mcall.close()
				signaled = True 


			data, address = call.recvfrom(4096)
			data = data.decode('utf-8')
			
			print("[RCV Connexion UDP:"+ str(PEERS_PORT)+"] " + data)
			if address[0] not in connected_hosts and address[0] != LOCAL_IP:
				if(data == PTC_CONNECT ):
					call.sendto(PTC_PRESENT.encode(),(BROADCAST_IP,PEERS_PORT))

				if(data == PTC_PRESENT ):
					print("Creation d'une connexion TCP avec " + str(address))

					newp = socket.socket()
					newp.connect((address[0], PEERS_PORT))
					self.connected.append(newp)
					connected_hosts.append(address[0])
					connected_socks.append(newp)
					listener = PeerTCPReceiver(newp)
					listener.start()
			

# Manage client requests
class ClientManager(Thread):
	def __init__(self, s, node):
		Thread.__init__(self)
		self.sock = s
		self.go = True
		self.node = node
	

	def run(self):

		global leClient
		global verrou
		global number

		while(self.go):
			data = self.sock.recv(1024)
			data = data.decode('utf-8').rstrip()
			if(data == ""):
				print("Client leaves")
				leClient.close()
				break
			data_cmd = data.split(" ")

			print("[CLI Request] <<< "+data)
			if( data_cmd[0] == PTC_CLI_ASK):
				
				number = data.split(' ')[1]
				verrou += 1
				print_state(number, verrou)
				req = PTC_ASK_PROMISE + " " + str(verrou)
				print("[SND Everyone] >>> " + req )
				PaxosNode.sendToAllNodes(req.encode())

			elif( data_cmd[0] == PTC_CLI_RELEASE_SRV):
				self.go = False
				leClient.close()

			elif(data_cmd[0] == PTC_STOP):
				print("Client try to kill me!!")
				self.go = False
				leClient.close()
				self.node.listenCli = False
				break
			else :
				print("<"+data+"> != " + PTC_CLI_ASK  )

	def sendData(self, msg):
		self.sock.send(msg.encode())


# Wait for client request and process
class PaxosNode(Thread):

	def __init__(self):
		Thread.__init__(self)
		self.rSock = socket.socket()
		self.udpFetcher = None
		self.tcpListen = None
		self.listenCli = True


	@staticmethod
	def sendToAllNodes(msg):

		for sock in connected_socks:
			sock.send(msg)

	def run(self):

		global leClient

		cli_port = CLIENTS_PORT
		srv_addr = LOCAL_IP

		self.udpFetcher = PeersUDPConnectionCollector()
		self.udpFetcher.start()

		self.tcpListen = PeersTCPConnectionCollector(srv_addr)
		self.tcpListen.start()

		sock = socket.socket()
		sock.bind((LOCAL_IP,CLIENTS_PORT))
		sock.listen(1)

		print("Waiting for clients ...")
		while(self.listenCli):
			s,client = getClient(sock)
			leClient = s
			cm = ClientManager(leClient,self)
			cm.start()
			cm.join()
			s.close()

		
def getClient(lesock):
	print("Listening for client on TCP:"+str(CLIENTS_PORT)+" ...")
	conn, client = lesock.accept()
	print("New client : " + str(client))
	return conn, client
			

client = PaxosNode()
client.start()
