# -*- coding: utf-8 -*-
"""
Created on Sat Dec  1 15:53:03 2018

@author: Asus
"""

import paramiko
client = None

try:
    # Connect to remote host
    key = paramiko.RSAKey.from_private_key_file("d:/Documents/Certs/id_rsa", password='Burkina!8faso')
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect('195.222.11.86', username='mgadmin', port=2222, pkey=key)

    # Setup sftp connection and transmit modules
    sftp = client.open_sftp()
    sftp.put('task.py', '/tmp/task.py')
    sftp.close()

    # Run the transmitted script remotely without args and show its output.
    # SSHClient.exec_command() returns the tuple (stdin,stdout,stderr)
    (inn, out, err) = client.exec_command(
            """export PATH="/home/mgadmin/miniconda3/bin:$PATH";
            source activate tf36;
            python /tmp/task.py;""")

    print('errors:')
    for line in err:
        # Process each line in the remote output
        print(line, end='')

    print('output:')
    for line in out:
        print(line, end='')
except:
    raise
finally:
    if client: client.close()