import socket


class GCPPreemptionDetector(object):
    def __init__(self):
        self.acpid_socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.acpid_socket.connect("/var/run/acpid.socket")
        self.acpid_socket.setblocking(0)

    def is_preempted(self):
        try:
            msg = self.acpid_socket.recv(4096).decode('utf-8')
            tokens = msg.split()
            # Google preempts machines by sending an ACPI G2 Soft Off signal.
            # That shows up on the ACPID socket as a string starting
            # with "button/power PBTN "
            return tokens[0] == 'button/power' and tokens[1] == 'PBTN'
        except socket.error:
            return False
