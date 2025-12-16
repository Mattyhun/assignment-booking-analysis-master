"""Windows compatibility fix for PySpark

This module applies a compatibility patch for PySpark on Windows.
PySpark tries to use UnixStreamServer which doesn't exist on Windows.
This patch adds a dummy class to prevent the AttributeError.

This module should be imported before any PySpark imports.
"""
import platform
import socketserver

# Apply Windows compatibility fix
if platform.system() == "Windows":
    if not hasattr(socketserver, 'UnixStreamServer'):
        # Create a dummy UnixStreamServer class for Windows compatibility
        class UnixStreamServer:
            """Dummy class for Windows compatibility with PySpark"""
            pass
        socketserver.UnixStreamServer = UnixStreamServer


