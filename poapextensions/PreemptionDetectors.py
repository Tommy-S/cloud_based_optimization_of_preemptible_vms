import socket


class GCEPreemptionDetector(object):
    """
    Mix-in class for preemptible workers on Google Compute Engine servers.
    Only works on unix-based machines.
    Detects ACPI G2 Soft-off events via a socket with the ACPID server.
    """

    def __init__(self):
        """Setup the socket with ACPID."""
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
            if tokens[0] == 'button/power' and tokens[1] == 'PBTN':
                self.acpid_socket.close()
                return True
            else:
                return False
        except socket.error:
            return False
