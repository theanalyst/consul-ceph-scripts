import socket
import struct
import json
import glob
import logging

logging.basicConfig(filename='/var/log/ceph/stats-osd-mon.log',
                    level=logging.INFO)


def query_osd_sock(osd_sock, query):
    try:
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.connect(osd_sock)
    except socket.error, e:
        logging.info("Connection with socket failed with %s" % e)
        return None

    sock.sendall(query)

    try:
        length = struct.unpack('>i', sock.recv(4))[0]
        json_data = json.loads(sock.recv(length))
    except Exception as e:
        logging.info("JSON parsing failed with %s" % e)
        return None
    return json_data


def main():
    socks = glob.glob('/var/run/ceph/*osd*asok')
    for sock in socks:
        r = query_osd_sock(sock, '{\"prefix\": \"perf dump\"}\0')
        if r is not None:
            print r
