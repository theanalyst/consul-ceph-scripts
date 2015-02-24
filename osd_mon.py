import socket
import struct
import json
import glob
import logging
import consulate

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


def process_socks(path):
    socks = glob.glob(path)
    for sock in socks:
        osd_id = sock.split(".")[-2]
        r = query_osd_sock(sock, '{\"prefix\": \"perf dump\"}\0')
        if r is not None:
            latency = r["filestore"]["journal_latency"]["sum"]
            consul().set("ceph/%s/latency" % osd_id, latency)


def consul(host='127.0.0.1', port=8500):
    return consulate.Consul(host, port)


def main():
    process_socks('/var/run/ceph/*osd*asok')

if __name__ == "__main__":
    main()
