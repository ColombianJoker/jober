#!/usr/bin/env python3
# encoding: utf-8
"""
davitrans.py: transmite desde y hacia unos destinos sftp según la configuración en una
  base de datos dada como parámetro.

Created by Ramón Barrios Láscar, 2025-02-21
"""

from datetime import datetime
from optparse import OptionParser, SUPPRESS_HELP
from time import sleep
import logging, logging.handlers
import os
import sqlite3
import string
import subprocess
import sys
import tempfile

def load_all_conf(dbfilename: str) -> ():
  """
  Load all configurations from SQLite database and return a tuple
  """
  cx = sqlite3.connect(dbfilename)
  cur = cx.cursor()
  global Options
  conf = None
  txs = None
  rxs = None

  # Load conn definitions
  try:
    try: # try to convert the connection argument to integer, use as text
         # if not possible
      Options.connection = int(Options.connection)
    except ValueError:
      pass
    if type(Options.connection) is int:
         # Try to get the connection number N given
      sql = f"SELECT id, cxname FROM cxdef WHERE id={Options.connection}"
      if Options.DEBUG:
        print(f"---→ {sql}")
      cur.execute(sql)
    else:
         # Try to get the connection with the text given
      sql = f"SELECT id, cxname FROM cxdef WHERE cxname LIKE '{Options.connection}'"
      if Options.DEBUG:
        print(f"---→ {sql}", file=sys.stderr)
      cur.execute(sql)
    cxdefs = cur.fetchone()
    if Options.DEBUG:
      print(f"<- {cxdefs=}", file=sys.stderr)
    if cxdefs:
      if Options.verbose:
        log.info(f"{Options.PrgName}: using connection definition #{cxdefs[0]} '{cxdefs[1]}'")
      cxid = cxdefs[0]
         # Try to get the directory to transfer up from
      sql = f"SELECT sourcedir, targetdir, archivedir, sftp FROM tx WHERE cxid={cxid} ORDER BY id"
      if Options.DEBUG:
        print(f"---→ {sql}", file=sys.stderr)
      cur.execute(sql)
      txs = cur.fetchall()
      if Options.DEBUG:
        print(f"<- {txs=}", file=sys.stderr)
         # Try to get the directory to transfer down from
      sql = f"SELECT sourcedir, targetdir, sftp FROM rx WHERE cxid={cxid} ORDER BY id"
      if Options.DEBUG:
        print(f"---→ {sql}", file=sys.stderr)
      cur.execute(sql)
      rxs = cur.fetchall()
      if Options.DEBUG:
        print(f"<- {rxs=}", file=sys.stderr)

    conf = (cxdefs, txs, rxs)
  except sqlite3.Error as e:
    print(f"An SQLite error occurred: {e}")
    return ()  # Return an empty list in case of error

  except Exception as e: # Catching potential other exceptions (file not found, etc.)
      print(f"{Options.PrgName}: a general error occurred {e=}")
      return ()
  return tuple(conf)

def log_filename(cx=None) -> str:
  """
  Return a log filename
  """
  if cx:
    log_file = os.path.join(os.getcwd(), os.path.splitext(os.path.basename(os.path.realpath(__file__)))[0]) + f".{cx[1]}.log"
  else:
    log_file = os.path.join(os.getcwd(), os.path.splitext(os.path.basename(os.path.realpath(__file__)))[0]) + f".log"
  return log_file

def set_logging(add_screen=True):
  """
  Configure logging
  """
  global Options
  if Options.dolog:
    Options.log = logging.getLogger("INODO")
    if Options.DEBUG:
      Options.log.setLevel(logging.DEBUG)
    else:
      Options.log.setLevel(logging.INFO)
    log_handler = logging.handlers.TimedRotatingFileHandler(Options.logfile, "Midnight", 1, backupCount=30)
    log_formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
    log_handler.setFormatter(log_formatter)
    if add_screen:
      screen_handler = logging.StreamHandler()
      screen_formatter = logging.Formatter("%(levelname)s %(message)s")
      screen_handler.setFormatter(screen_formatter)
    # Clear handlers and re-add
    # Clear
    for a_handler in Options.log.handlers:
      Options.log.removeHandler(a_handler)
    # Re-add
    Options.log.addHandler(log_handler)
    if add_screen:
      Options.log.addHandler(screen_handler)
    return Options.log

def transmit_one_scp(cx, tx, the_file):
  """
  Try to transmit one file using SCP
  cx       has the connection data. Must match something in $HOME/.ssh/config
  tx       has the data for transmissions: local directory source, remote directory
           target, local archive directory
  the_file has the basename of the file to transmit
  """
  cmd = Options.scp
  source_file = os.path.join(tx[0], the_file)
  full_cmd = f"{cmd} {source_file} {cx}:{tx[1]}"
  rc = 0
  if Options.DEBUG:
    log.debug(f"---→ '{full_cmd}'")
  try:                                      # Try to transmit
    cx_lines = ""
    cx_lines = subprocess.check_output(full_cmd.split(), shell=False)
    cx_lines = cx_lines.decode('utf-8')
    if Options.DEBUG and (len(cx_lines)>0):
      log.debug(f"---→ {cx_lines}")
      sys.stderr.flush()
    if not Options.DEBUG:
      log.info(f"-> {source_file} => {cx}:{tx[1]}")
  except subprocess.CalledProcessError as pe:
    if pe.returncode!=0:
      log.info(f"---→ using '{full_cmd}' returned {pe.returncode}")
      rc = pe.returncode
  try:                                      # Try to move
    if Options.DEBUG:
      log.debug(f"mv '{source_file}' '{os.path.join(tx[2], the_file)}'")
    os.rename(source_file, os.path.join(tx[2], the_file)) # tx[2] == arch directory
    if Options.DEBUG:
      log.debug(f"'{source_file}' moved")
  except:
    log.error(f"Could not move {source_file} to {tx[2]}")
  return rc

def transmit_one_sftp(cx, tx, the_file):
  """
  Try to transmit one file using SFTP
  cx       has the connection data. Must match something in $HOME/.ssh/config
  tx       has the data for transmissions: local directory source, remote directory
           target, local archive directory
  the_file has the basename of the file to transmit
  """
  cmd = Options.sftp
  rc = 0
  source_file = os.path.join(tx[0], the_file)
  temp = tempfile.NamedTemporaryFile(delete=False, dir=Options.tmpdir)
  # temp = tempfile.NamedTemporaryFile(delete=False)
  full_cmd = f"{cmd} -b {temp.name} {cx}"
  try:
    sftp_cmd = f"put {source_file} {tx[1]}".encode("utf-8")
    if Options.DEBUG:
      log.debug(f"---> {sftp_cmd}")
      log.debug(f"---→ '{full_cmd}'")
    with open(temp.name, "wb") as tmpfile:
      tmpfile.write(sftp_cmd)
      tmpfile.close()
    try:
      cx_lines = ""
      cx_lines = subprocess.check_output(full_cmd.split(), shell=False)
      cx_lines = cx_lines.decode('utf-8')
      if Options.DEBUG and (len(cx_lines)>0):
        log.debug(f"---→ {cx_lines}")
        sys.stderr.flush()
      if not Options.DEBUG:
        sftp_cmd = sftp_cmd.decode("utf-8")
        log.info(f"-> {sftp_cmd}")
      try:                                      # Try to move
        if Options.DEBUG:
          log.debug(f"mv '{source_file}' '{os.path.join(tx[2], the_file)}'")
        os.rename(source_file, os.path.join(tx[2], the_file)) # tx[2] == arch directory
        if Options.DEBUG:
          log.debug(f"'{source_file}' moved")
      except:
        log.error(f"Could not move {source_file} to {tx[2]}")
      try:
        os.unlink(temp.name)
      except:
        log.error(f"Could not remove temporary file '{temp.name}'")
    except subprocess.CalledProcessError as pe:
      if pe.returncode!=0:
        log.info(f"---→ using '{full_cmd}' in {temp.name} returned {pe.returncode}")
        rc = pe.returncode
  except IOError as e:
    print(f"{e=}", file=sys.stderr)
    if Options.DEBUG:
      log.debug(f"Could not write temporary file '{temp.name}'")
    else:
      log.error(f"Could not write temporary file to {Options.tmpdir}")
    rc =1
  return rc

def transmit_all(cx, txs):
  """
  Do a transmission set
  cx  has the connection data. Must match something in $HOME/.ssh/config
  txs has the list of settings for transmissions: source local directories,
      target remote directories, local archive directories
  """
  global Options

  if Options.DEBUG:
    log.debug(f"---→ Trying to transmit ...")
    log.debug(f"---→ {cx=}")
    log.debug(f"---→ {txs=}")
  for tx in txs:
    if Options.DEBUG:
      log.debug(f"---→ {tx=}")
    if os.path.isdir(tx[0]):
      a_dir = tx[0]
      if Options.DEBUG:
        log.debug(f"Directory '{a_dir}'")
      for start_dir, dirs, files in os.walk(a_dir):
        for one_file in files:
          if tx[3]==0:
            # Transmit using SCP
            transmit_one_scp(cx[1], tx, one_file)
          else: # tx[3]==1
            transmit_one_sftp(cx[1], tx, one_file)
        else:
          if Options.DEBUG:
            log.debug(f"---→ '{a_dir}' found empty")
  return

def receive_one_scp(cx, rx, a_file):
  """
  Receive a file using SCP
  """
  global Options
  rc = 0

  # if Options.DEBUG:
  #   print(f"---→ {cx=}, {rx=}, {a_file=}")
  full_cmd = [ Options.scp, os.path.join(f"{cx[1]}:{rx[0]}", a_file), rx[1], ]
  if Options.DEBUG:
    log.debug(f"-→ {full_cmd=}")
  try:                    # Try to get one
    rx_lines = ""
    rx_lines = subprocess.check_output(full_cmd, shell=False)
    rx_lines = rx_lines.decode('utf-8')
    if len(rx_lines)>0:
      log.debug(f"<- {rx_lines=}")
    full = os.path.join(f"{cx[1]}:{rx[0]}", a_file)
    log.info(f"<- {full} => {rx[1]}")
  except subprocess.CalledProcessError as pe:
    if pe.returncode!=0:
      log.info(f"---→ using '{full_cmd}' returned {pe.returncode}")
      rc = pe.returncode
  return rc

def remove_one_scp(cx, rx, a_file):
  """
  Remove a file using SCP
  """
  global Options
  rc = 0

  # if Options.DEBUG:
  #   print(f"---→ {cx=}, {rx=}, {a_file=}")
  full_cmd = [ Options.ssh, cx[1], "rm", os.path.join(rx[0], a_file), ]
  if Options.DEBUG:
    print(f"----> {full_cmd=}", file=sys.stderr)
  try:                  # Try to remove one
    x_lines = ""
    x_lines = subprocess.check_output(full_cmd, shell=False)
    x_lines = x_lines.decode('utf-8')
    if len(x_lines)>0:
      log.debug(f"-> {x_lines}")
  except subprocess.CalledProcessError as pe:
    if pe.returncode!=0:
      log.info(f"---→ using '{full_cmd}' returned {pe.returncode}", file=sys.stderr)
      rc = rc + pe.returncode
  return rc

def receive_one_sftp(cx, rx, a_file):
  """
  Receive a file using sftp
  """
  global Options
  rc = 0
  if Options.DEBUG:
    log.debug(f"---→ {cx=}, {rx=}, {a_file=}")
  temp = tempfile.NamedTemporaryFile(delete=False, dir=Options.tmpdir)
  full_cmd = f"{Options.sftp} -b {temp.name} {cx[1]}"
  try:
    sftp_cmd = f"get {a_file} {rx[1]}".encode("utf-8")
    with open(temp.name, "wb") as tmpfile:
      tmpfile.write(sftp_cmd)
      tmpfile.close()
    if Options.DEBUG:
        log.debug(f"---→ {sftp_cmd=}")
        log.debug(f"---→ {full_cmd=}")
    else:
      sftp_cmd = sftp_cmd.decode("utf-8")
      log.info(f"---→ {sftp_cmd}")
    try:                  # Try to download one
      rx_lines = ""
      rx_lines = subprocess.check_output(full_cmd.split(), shell=False)
      rx_lines = rx_lines.decode('utf-8')
      if len(rx_lines)>0:
        log.debug(f"-> {rx_lines}")
    except subprocess.CalledProcessError as pe:
      if pe.returncode!=0:
        log.info(f"---→ using '{full_cmd}' returned {pe.returncode}", file=sys.stderr)
        rc = rc + pe.returncode
    try: # to remove temporary file
      os.unlink(temp.name)
    except:
      log.error(f"Could not remove temporary file '{temp.name}'")
  except IOError as e:
    print(f"{e=}", file=sys.stderr)
    if Options.DEBUG:
      log.debug(f"Could not write temporary file '{temp.name}'")
    else:
      log.error(f"Could not write temporary file to {Options.tmpdir}")
    rc = rc + 1
  return rc

def remove_one_sftp(cx, rx, a_file):
  """
  Remove a file using sftp
  """
  global Options
  rc = 0
  if Options.DEBUG:
    log.debug(f"---→ {cx=}, {rx=}, {a_file=}")
  temp = tempfile.NamedTemporaryFile(delete=False, dir=Options.tmpdir)
  full_cmd = f"{Options.sftp} -b {temp.name} {cx[1]}"
  try:
    sftp_cmd = f"rm {a_file}".encode("utf-8")
    with open(temp.name, "wb") as tmpfile:
      tmpfile.write(sftp_cmd)
      tmpfile.close()
    if Options.DEBUG:
        log.debug(f"---→ {sftp_cmd=}")
        log.debug(f"---→ {full_cmd=}")
    else:
      sftp_cmd = sftp_cmd.decode("utf-8")
      log.info(f"---→ {sftp_cmd}")
    try:                  # Try to remove one
      rx_lines = ""
      rx_lines = subprocess.check_output(full_cmd.split(), shell=False)
      rx_lines = rx_lines.decode('utf-8')
      if len(rx_lines)>0:
        log.debug(f"-> {rx_lines}")
    except subprocess.CalledProcessError as pe:
      if pe.returncode!=0:
        log.info(f"---→ using '{full_cmd}' returned {pe.returncode}", file=sys.stderr)
        rc = rc + pe.returncode
    try: # to remove temporary file
      os.unlink(temp.name)
    except:
      log.error(f"Could not remove temporary file '{temp.name}'")
  except IOError as e:
    print(f"{e=}", file=sys.stderr)
    if Options.DEBUG:
      log.debug(f"Could not write temporary file '{temp.name}'")
    else:
      log.error(f"Could not write temporary file to {Options.tmpdir}")
    rc = rc + 1
  return rc

def receive_all(cx, rxs):
  """
  Do a reception set
  """
  global Options
  rc = 0
  
  def desanida_lista(lista_lista):
    return [item for sublist in lista_lista for item in sublist]

  if Options.DEBUG:
    print(f"---→ Trying to receive ...", file=sys.stderr)
    print(f"---→ {cx=}", file=sys.stderr)
  for rx in rxs:
    if rx[2]==0: # receive by SCP
      cmd = [ Options.ssh, cx[1], "ls", ]
      full_cmd = list(cmd)
      full_cmd.append(rx[0])
      if Options.DEBUG:
        log.debug(f"---→ {rx=}")
        log.debug(f"---→ {full_cmd=}")
      try:                   # Try to list
        rx_lines = ""
        rx_lines = subprocess.check_output(full_cmd, shell=False)
        rx_lines = rx_lines.decode('utf-8')
        if len(rx_lines)>0:
          rx_lines = rx_lines.splitlines()
          if Options.DEBUG:
            log.debug(f"<- {rx_lines}")
            sys.stderr.flush()
          for a_file in rx_lines:
            rorc = receive_one(cx, rx, a_file)
            if rorc==0:
              remove_one(cx, rx, a_file)
        else:
          if Options.DEBUG:
            log.debug(f"---→ {rx[0]} found empty.")
      except subprocess.CalledProcessError as pe:
        if pe.returncode!=0:
          log.info(f"---→ using '{full_cmd}' returned {pe.returncode}")
          rc = rc + pe.returncode
    else: # receive by SFTP
      temp = tempfile.NamedTemporaryFile(delete=False, dir=Options.tmpdir)
      full_cmd = f"{Options.sftp} -b {temp.name} {cx[1]}"
      try:
        sftp_cmd = f"ls {rx[0]}".encode("utf-8")
        if Options.DEBUG:
          log.debug(f"---→ {full_cmd=}")
          log.debug(f"---→ {sftp_cmd=}")
        with open(temp.name, "wb") as tmpfile:
          tmpfile.write(sftp_cmd)
          tmpfile.close()
        try:
          cx_lines = ""
          cx_lines = subprocess.check_output(full_cmd.split(), shell=False)
          cx_lines = cx_lines.decode('utf-8')
          if len(cx_lines)>0:
            source_lines = cx_lines.splitlines()[1:]
            source_files = []
            for a_source_line in source_lines:
              source_files.append(a_source_line.split())
            source_files = desanida_lista(source_files)
            if Options.DEBUG:
              log.debug(f"<--- {cx_lines}")
            for a_source in source_files:
              # log.info(f"<--- {a_source}")
              if receive_one_sftp(cx, rx, a_source)==0:
                remove_one_sftp(cx, rx, a_source)
        except subprocess.CalledProcessError as pe:
          if pe.returncode!=0:
            log.info(f"---→ using '{full_cmd}' returned {pe.returncode}")
            rc = rc + pe.returncode
        try: # to remove temporary file
          os.unlink(temp.name)
        except:
          log.error(f"Could not remove temporary file '{temp.name}'")
      except IOError as e:
        print(f"{e=}", file=sys.stderr)
        if Options.DEBUG:
          log.debug(f"Could not write temporary file '{temp.name}'")
        else:
          log.error(f"Could not write temporary file to {Options.tmpdir}")
        rc = rc + 1
  return rc

# START OF MAIN FILE
try:
  parser = OptionParser(usage="%prog --OPTIONS CONFIGURATIONFILE")
  parser.add_option("--verbose", "-v", dest="verbose", action="store_true", help="Verbose mode", default=False)
  parser.add_option("--seconds", "--segundos", dest="seconds", action="store_true", help="Run with a period of seconds", default=False)
  parser.add_option("-w", "--wait", "--espera", dest="wait", action="store", help="Wait time units", type="int", default=5)
  parser.add_option("-C", "--cx", "--connection", dest="connection", action="store", help="Connection filter to use", default=None)
  parser.add_option("--scp-bin", "--scp", dest="scp", action="store", help=SUPPRESS_HELP, default="/usr/bin/scp")
  parser.add_option("--sftp-bin", "--sftp", dest="sftp", action="store", help=SUPPRESS_HELP, default="/usr/bin/sftp")
  parser.add_option("--ssh-bin", "--ssh", dest="ssh", action="store", help=SUPPRESS_HELP, default="/usr/bin/ssh")
  parser.add_option("--dont-move", dest="move", action="store_false", help=SUPPRESS_HELP, default=True)
  parser.add_option("--no-log", "--dont-log", dest="dolog", action="store_false", help="Don't log to a file", default=True)
  parser.add_option("-o", "--output", dest="logfile", action="store", type="string", help="Log execution into file name")
  parser.add_option("--tmp", dest="tmp", action="store", type="string", help="Temporary directory, defaults to /tmp", default="/tmp")
  parser.add_option("--DEBUG", dest="DEBUG", action="store_true", help=SUPPRESS_HELP, default=False)

  (Options, Args) = parser.parse_args()
  Options.PrgName = "Davitrans"

  if Options.DEBUG:
    Options.verbose = False
    print(f"---→ Start of execution", file=sys.stderr)
    print(f"---→ {Options=}", file=sys.stderr)

  if Options.verbose:
    print(f"{Options.PrgName} starting at {datetime.now()}")
  
  Options.tmpdir = os.environ.get("TMPDIR", default=Options.tmp)
  if Options.DEBUG:
    print(f"{Options.PrgName} using '{Options.tmpdir}' for temporary files")
  
  if Options.dolog:
    if Options.logfile:
      pass
    else:
      Options.logfile=log_filename()
      if Options.DEBUG:
        print(f"{Options.PrgName}: {Options.logfile=}")
  log = set_logging()

  if len(Args)!=1:
    log.critical(f"{PrgName}: no configuration file given, exiting ...")
    sys.exit(1)
  else:
    confdb = Args[0]
    log.info(f"{Options.PrgName}: starting execution")
    log.info(f"{Options.PrgName}: configuration file '{confdb}'")
    log.info(f"{Options.PrgName}: will check for files to transmit each {Options.wait} %s"%("seconds" if Options.seconds else "minutes",))
    if not Options.connection:
      log.critical(f"{Options.PrgName}: Connection selector not given, exiting...")
      sys.exit(2)
    if not os.path.isfile(confdb):
      log.critical(f"{Options.PrgName}: Could not use '{confdb}', exiting...")
      sys.exit(3)
    conf = load_all_conf(confdb)
    if Options.DEBUG:
      log.debug(f"{Options.PrgName}: {conf=}")
    # Re-set logging
    Options.logfile = log_filename(conf[0])
    log.info(f"{Options.PrgName} changing to new log file '{Options.logfile}'")
    log = None
    log = set_logging(add_screen=False)
    log.info(f"{Options.PrgName} changed to new log file '{Options.logfile}'")
    
    wait = Options.wait if Options.seconds else 60*Options.wait
    if Options.DEBUG:
      unit = "s" if Options.seconds else "m"
      log.debug(f"---→ Waiting for {Options.wait}{unit}")
    while True:
      if Options.DEBUG:
        print("\n")
      print(f"{datetime.now()}")
      transmit_all(conf[0], conf[1])
      receive_all(conf[0], conf[2])
      sleep(wait)
except KeyboardInterrupt:
  log.critical(f"{Options.PrgName}: Process cancelled!")
  # print(f"\n{Options.PrgName}: Process cancelled!\n", file=sys.stderr)
  sys.stderr.flush()
  sys.exit(1)
