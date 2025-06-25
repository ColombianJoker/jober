#!/usr/bin/env python3.11

from datetime import datetime
from fabric import Connection
from paramiko.ssh_exception import AuthenticationException, NoValidConnectionsError
from invoke.exceptions import UnexpectedExit
from optparse import OptionParser, SUPPRESS_HELP
import sys
import time

# MAIN
try:
    parser = OptionParser(usage="%prog --OPTIONS CONFIGURATIONFILE")
    parser.add_option("--DEBUG", dest="DEBUG", action="store_true", help=SUPPRESS_HELP, default=False)
    parser.add_option("-t", "--timeout", dest="timeout", action="store", type="int", help="SSH connection timeout", default=10)
    parser.add_option("-T", "--auth-timeout", dest="auth_timeout", action="store", type="int", help="SSH authentication timeout", default=5)
    parser.add_option("-U", "--user", "--username", dest="username", action="store", help="Username to try to connect to", default="root")
    parser.add_option("-C", "--command", dest="command", action="store", help="Command to execute", default="exit 0")
    parser.add_option("-H", "--hide", dest="hide", action="store_true", help="Hide connections", default=False)

    (Options, Args) = parser.parse_args()
    Options.PrgName = "paramigo"

    if Options.DEBUG:
        Options.verbose = False
        print(f"---→ Start of execution", file=sys.stderr)
        print(f"---→ Options=%s"%(Options,), file=sys.stderr)
        print(f"---→ Args=%s"%(Args,), file=sys.stderr)

    if len(Args):
        print(f"{Options.PrgName} starting at {datetime.now():}", file=sys.stderr)
        for one_host in Args:
            start_time = time.time()
            try:
                res = Connection(one_host, user=Options.username,
                  connect_kwargs = {
                    "timeout": Options.timeout,
                    "auth_timeout": Options.auth_timeout,
                  }
                ).run(Options.command, hide=Options.hide)
                duration = time.time() - start_time
                if Options.hide:
                    print(f"{one_host}\t{duration:.3f}s")
                else:
                    print(f"{one_host}:\t{res.stdout.strip()}\t{duration:.3f}s")
            except UnexpectedExit:
                if Options.hide:
                    print(f"{one_host}\t{duration:.3f}s")
                else:
                  print(f"{one_host}:\t{res.stdout.strip()}\t{duration:.3f}s")
            except (AuthenticationException, NoValidConnectionsError, TimeoutError, OSError):
                pass
        print(f"{Options.PrgName} ending at {datetime.now():}", file=sys.stderr)
  
except KeyboardInterrupt:
    sys.stderr.write(f"\n{Options.PrgName}: Process cancelled!\n")
    sys.stderr.flush()
    sys.exit(1)

