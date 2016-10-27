"""
Performs basic validation after successful compilation
"""

import subprocess, os, time, socket
import subprocess32 as subprocess
s = socket.socket()
s.bind(('', 0))
free_port = str(s.getsockname()[1])
s.close()

build_output = subprocess.check_output(['ant', 'build'])
print build_output

if b'BUILD SUCCESSFUL' not in build_output:
	print ('The program failed to build.')
	exit(-1)

try:
	crawl_output = subprocess.check_output(['ant', 'crawl'], timeout = 30)
	print crawl_output
except subprocess.TimeoutExpired as e:
	crawl_output = e.output
	if (b'http://crawltest.cis.upenn.edu: Downloading' in crawl_output) or (b'http://crawltest.cis.upenn.edu/: Downloading'in crawl_output) or (b'http://crawltest.cis.upenn.edu:80/: Downloading'in crawl_output):
		print ('The program passed validation!')
		exit(0)
	else:
		print ('The program failed to download the sandbox landing page.')
		exit(-1)

print ('Ant crawl did not work/ program did not respect robots.txt')
exit(-1)


