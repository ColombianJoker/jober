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
      sql = f"SELECT id, cxname FROM cxdef WHERE id=%s"%(Options.connection,)
      if Options.DEBUG:
        print(f"---→ %s"%(sql,))
      cur.execute(sql)
    else:
         # Try to get the connection with the text given
      sql = f"SELECT id, cxname FROM cxdef WHERE cxname LIKE '%s'"%(Options.connection,)
      if Options.DEBUG:
        print(f"---→ %s"%(sql,), file=sys.stderr)
      cur.execute(sql)
    cxdefs = cur.fetchone()
    if Options.DEBUG:
      print(f"<- cxdefs='%s'"%(cxdefs,), file=sys.stderr)
    if cxdefs:
      if Options.verbose:
        log.info(f"%s: using connection definition #%s '%s'"%(Options.PrgName, cxdefs[0], cxdefs[1]))
      cxid = cxdefs[0]
         # Try to get the directory to transfer up from
      sql = f"SELECT sourcedir, targetdir, archivedir, sftp FROM tx WHERE cxid=%d ORDER BY id"%(cxid,)
      if Options.DEBUG:
        print(f"---→ '%s'"%(sql,), file=sys.stderr)
      cur.execute(sql)
      txs = cur.fetchall()
      if Options.DEBUG:
        print(f"<- txs='%s'"%(txs,), file=sys.stderr)
         # Try to get the directory to transfer down from
      sql = f"SELECT sourcedir, targetdir, sftp FROM rx WHERE cxid=%d ORDER BY id"%(cxid,)
      if Options.DEBUG:
        print(f"---→ '%s'"%(sql,), file=sys.stderr)
      cur.execute(sql)
      rxs = cur.fetchall()
      if Options.DEBUG:
        print(f"<- rxs='%s'"%(rxs,), file=sys.stderr)

    conf = (cxdefs, txs, rxs)
  except sqlite3.Error as e:
    print(f"An SQLite error occurred: e='%s'"%(e,))
    return ()  # Return an empty list in case of error

  except Exception as e: # Catching potential other exceptions (file not found, etc.)
      print(f"A general error occurred e='%s'"%(e,))
      return ()
  return tuple(conf)

def log_filename(cx=None) -> str:
  """
  Return a log filename
  """
  if cx:
    log_file = os.path.join(os.getcwd(), os.path.splitext(os.path.basename(os.path.realpath(__file__)))[0]) + f".%s.log"%(cx[1],)
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
    log_handler = logging.handlers.TimedRotatingFileHandler(Options.logfile,
      when="M",        # Rotar mensualmente
      interval=1,      # Logs de a 1 mes
      backupCount=36,  # 36 meses o 3 años de logs
    )
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
  full_cmd = f"%s %s %s:"%(cmd, source_file, cx, tx[1],)
  rc = 0
  if Options.DEBUG:
    log.debug(f"---→ '%s'"%(full_cmd,))
  try:                                      # Try to transmit
    cx_lines = ""
    cx_lines = subprocess.check_output(full_cmd.split(), shell=False)
    cx_lines = cx_lines.decode('utf-8')
    if Options.DEBUG and (len(cx_lines)>0):
      log.debug(f"---→ %s"%(cx_lines,))
      sys.stderr.flush()
    if not Options.DEBUG:
      log.info(f"-> %s => %s:%s"%(source_file, cx, tx[1],))
  except subprocess.CalledProcessError as pe:
    if pe.returncode!=0:
      log.info(f"---→ using '%s' returned %d"%(full_cmd, pe.returncode,))
      rc = pe.returncode
  try:                                      # Try to move
    if Options.DEBUG:
      log.debug(f"mv '%s' '%s'"%(source_file, os.path.join(tx[2], the_file),))
    os.rename(source_file, os.path.join(tx[2], the_file)) # tx[2] == arch directory
    if Options.DEBUG:
      log.debug(f"'%s' moved"%(source_file,))
  except:
    log.error(f"Could not move '%s' to '%s'"%(source_file, tx[2],))
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
  full_cmd = f"%s -b %s %s"%(cmd, temp.name, cx)
  try:
    sftp_cmd = (f"put %s %s"%(source_file, tx[1],)).encode("utf-8")
    if Options.DEBUG:
      log.debug(f"---> '%s'"%(sftp_cmd,))
      log.debug(f"---→ '%s'"%(full_cmd,))
    with open(temp.name, "wb") as tmpfile:
      tmpfile.write(sftp_cmd)
      tmpfile.close()
    try:
      cx_lines = ""
      cx_lines = subprocess.check_output(full_cmd.split(), shell=False)
      cx_lines = cx_lines.decode('utf-8')
      if Options.DEBUG and (len(cx_lines)>0):
        log.debug(f"---→ %s"%(cx_lines,))
        sys.stderr.flush()
      if not Options.DEBUG:
        sftp_cmd = sftp_cmd.decode("utf-8")
        log.info(f"-> %s"%(sftp_cmd,))
      try:                                      # Try to move
        if Options.DEBUG:
          log.debug(f"mv '%s' '%s'"%(source_file, os.path.join(tx[2], the_file),))
        os.rename(source_file, os.path.join(tx[2], the_file)) # tx[2] == arch directory
        log.info(f"'%s' moved to '%s'"%(source_file, os.path.join(tx[2], the_file),))
        if Options.DEBUG:
          log.debug(f"'%s' moved"%(source_file,))
      except:
        log.error(f"Could not move '%s' to '%s'"%(source_file, tx[2],))
      try:
        os.unlink(temp.name)
      except:
        log.error(f"Could not remove temporary file '%s'"%(temp.name,))
    except subprocess.CalledProcessError as pe:
      if pe.returncode!=0:
        log.info(f"---→ using '%s' in %s returned %d"%(full_cmd, temp.name, pe.returncode,))
        rc = pe.returncode
  except IOError as e:
    print(f"A general IO error ocurred e='%s'"%(e,), file=sys.stderr)
    if Options.DEBUG:
      log.debug(f"Could not write temporary file '%s'"%(temp.name,))
    else:
      log.error(f"Could not write temporary file to directory '%s'"%(Options.tmpdir,))
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
    log.debug(f"---→ cx='%s'"%(cx,))
    log.debug(f"---→ txs='%s'"%(txs,))
  for tx in txs:
    if Options.DEBUG:
      log.debug(f"---→ tx='%s'"%(tx,))
    if os.path.isdir(tx[0]):
      a_dir = tx[0]
      if Options.DEBUG:
        log.debug(f"Directory '%s'"%(a_dir,))
      for start_dir, dirs, files in os.walk(a_dir):
        for one_file in files:
          if tx[3]==0:
            # Transmit using SCP
            transmit_one_scp(cx[1], tx, one_file)
          else: # tx[3]==1
            transmit_one_sftp(cx[1], tx, one_file)
        else:
          if Options.DEBUG:
            log.debug(f"---→ '%s' found empty"%(a_dir,))
  return

def receive_one_scp(cx, rx, a_file):
  """
  Receive a file using SCP
  """
  global Options
  rc = 0

  full_cmd = [ Options.scp, os.path.join(f"%s:%s"%(cx[1], rx[0],), a_file), rx[1], ]
  if Options.DEBUG:
    log.debug(f"-→ full_cmd='%s'"%(full_cmd,))
  try:                    # Try to get one
    rx_lines = ""
    rx_lines = subprocess.check_output(full_cmd, shell=False)
    rx_lines = rx_lines.decode('utf-8')
    if len(rx_lines)>0:
      log.debug(f"<- rx_lines='%s'"%(rx_lines,))
    full = os.path.join(f"%s:%s"%(cx[1], rx[0],), a_file)
    log.info(f"<- %s => %s"%(full, rx[1]))
  except subprocess.CalledProcessError as pe:
    if pe.returncode!=0:
      log.info(f"---→ using '%s' returned %d"%(full_cmd, pe.returncode,))
      rc = pe.returncode
  return rc

def remove_one_scp(cx, rx, a_file):
  """
  Remove a file using SCP
  """
  global Options
  rc = 0

  full_cmd = [ Options.ssh, cx[1], "rm", os.path.join(rx[0], a_file), ]
  if Options.DEBUG:
    print(f"----> full_cmd='%s'"%(full_cmd,), file=sys.stderr)
  try:                  # Try to remove one
    x_lines = ""
    x_lines = subprocess.check_output(full_cmd, shell=False)
    x_lines = x_lines.decode('utf-8')
    if len(x_lines)>0:
      log.debug(f"-> %s"%(x_lines,))
  except subprocess.CalledProcessError as pe:
    if pe.returncode!=0:
      log.info(f"---→ using '%s' returned %d"%(full_cmd, pe.returncode,), file=sys.stderr)
      rc = rc + pe.returncode
  return rc

def receive_one_sftp(cx, rx, a_file):
  """
  Receive a file using sftp
  """
  global Options
  rc = 0
  if Options.DEBUG:
    log.debug(f"---→ cx='%s', rx='%s', a_file='%s'"%(cx, rx, a_file,))
  temp = tempfile.NamedTemporaryFile(delete=False, dir=Options.tmpdir)
  full_cmd = f"%s -b %s %s"%(Options.sftp, temp.name, cx[1],)
  try:
    sftp_cmd = (f"get %s %s"%(a_file, rx[1],)).encode("utf-8")
    with open(temp.name, "wb") as tmpfile:
      tmpfile.write(sftp_cmd)
      tmpfile.close()
    if Options.DEBUG:
        log.debug(f"---→ sftp_cmd='%s'"%(sftp_cmd,))
        log.debug(f"---→ full_cmd='%s'"%(full_cmd,))
    else:
      sftp_cmd = sftp_cmd.decode("utf-8")
      log.info(f"---→ %s"%(sftp_cmd,))
    try:                  # Try to download one
      rx_lines = ""
      rx_lines = subprocess.check_output(full_cmd.split(), shell=False)
      rx_lines = rx_lines.decode('utf-8')
      if len(rx_lines)>0:
        log.debug(f"-> %s"%(rx_lines,))
    except subprocess.CalledProcessError as pe:
      if pe.returncode!=0:
        log.info(f"---→ using '%s' returned %d"%(full_cmd, pe.returncode,), file=sys.stderr)
        rc = rc + pe.returncode
    try: # to remove temporary file
      os.unlink(temp.name)
    except:
      log.error(f"Could not remove temporary file '%s'"%(temp.name,))
  except IOError as e:
    print(f"A general IO error ocurred e='%s'"%(e,), file=sys.stderr)
    if Options.DEBUG:
      log.debug(f"Could not write temporary file '%s'"%(temp.name,))
    else:
      log.error(f"Could not write temporary file to directory '%s'"%(Options.tmpdir,))
    rc = rc + 1
  return rc

def remove_one_sftp(cx, rx, a_file):
  """
  Remove a file using sftp
  """
  global Options
  rc = 0
  if Options.DEBUG:
    log.debug(f"---→ cx='%s', rx='%s', a_file='%s'"%(cx, rx, a_file,))
  temp = tempfile.NamedTemporaryFile(delete=False, dir=Options.tmpdir)
  full_cmd = f"%s -b %s %s"%(Options.sftp, temp.name, cx[1],)
  try:
    sftp_cmd = (f"rm %s"%(a_file,)).encode("utf-8")
    with open(temp.name, "wb") as tmpfile:
      tmpfile.write(sftp_cmd)
      tmpfile.close()
    if Options.DEBUG:
        log.debug(f"---→ sftp_cmd='%s'"%(sftp_cmd,))
        log.debug(f"---→ full_cmd='%s'"%(full_cmd,))
    else:
      sftp_cmd = sftp_cmd.decode("utf-8")
      log.info(f"---→ %s"%(sftp_cmd,))
    try:                  # Try to remove one
      rx_lines = ""
      rx_lines = subprocess.check_output(full_cmd.split(), shell=False)
      rx_lines = rx_lines.decode('utf-8')
      if len(rx_lines)>0:
        log.debug(f"-> %s"%(rx_lines,))
    except subprocess.CalledProcessError as pe:
      if pe.returncode!=0:
        log.info(f"---→ using '%s' returned %d"%(full_cmd, pe.returncode,), file=sys.stderr)
        rc = rc + pe.returncode
    try: # to remove temporary file
      os.unlink(temp.name)
    except:
      log.error(f"Could not remove temporary file '%s'"%(temp.name,))
  except IOError as e:
    print(f"A general IO error ocurred e='%s'"%(e,), file=sys.stderr)
    if Options.DEBUG:
      log.debug(f"Could not write temporary file '%s'"%(temp.name,))
    else:
      log.error(f"Could not write temporary file to directory '%s'"%(Options.tmpdir,))
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
    print(f"---→ cx='%s'"%(cx,), file=sys.stderr)
  for rx in rxs:
    if rx[2]==0: # receive by SCP
      cmd = [ Options.ssh, cx[1], "ls", ]
      full_cmd = list(cmd)
      full_cmd.append(rx[0])
      if Options.DEBUG:
        log.debug(f"---→ rx='%s'"%(rx,))
        log.debug(f"---→ full_cmd='%s'"%(full_cmd,))
      try:                   # Try to list
        rx_lines = ""
        rx_lines = subprocess.check_output(full_cmd, shell=False)
        rx_lines = rx_lines.decode('utf-8')
        if len(rx_lines)>0:
          rx_lines = rx_lines.splitlines()
          if Options.DEBUG:
            log.debug(f"<- %s"%(rx_lines,))
            sys.stderr.flush()
          for a_file in rx_lines:
            rorc = receive_one(cx, rx, a_file)
            if rorc==0:
              remove_one(cx, rx, a_file)
        else:
          if Options.DEBUG:
            log.debug(f"---→ %s found empty."%(rx[0],))
      except subprocess.CalledProcessError as pe:
        if pe.returncode!=0:
          log.info(f"---→ using '%s' returned %d"%(full_cmd, pe.returncode,))
          rc = rc + pe.returncode
    else: # receive by SFTP
      temp = tempfile.NamedTemporaryFile(delete=False, dir=Options.tmpdir)
      full_cmd = f"%s -b %s %s"%(Options.sftp, temp.name, cx[1],)
      try:
        sftp_cmd = (f"ls %s"%(rx[0])).encode("utf-8")
        if Options.DEBUG:
          log.debug(f"---→ full_cmd='%s'"%(full_cmd,))
          log.debug(f"---→ sftp_cmd='%s'"%(sftp_cmd,))
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
              log.debug(f"<--- %s"%(cx_lines,))
            for a_source in source_files:
              if receive_one_sftp(cx, rx, a_source)==0:
                remove_one_sftp(cx, rx, a_source)
        except subprocess.CalledProcessError as pe:
          if pe.returncode!=0:
            log.info(f"---→ using '%s' returned %d"%(full_cmd, pe.returncode,))
            rc = rc + pe.returncode
        try: # to remove temporary file
          os.unlink(temp.name)
        except:
          log.error(f"%s: Could not remove temporary file '%s'"%(Options.PrgName, temp.name,))
      except IOError as e:
        print(f"A general IO error ocurred e='%s'"%(e,), file=sys.stderr)
        if Options.DEBUG:
          log.debug(f"Could not write temporary file '%s'"%(temp.name,))
        else:
          log.error(f"Could not write temporary file to directory '%s'"%(Options.tmpdir,))
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
    print(f"---→ Options=%s"%(Options,), file=sys.stderr)
    print(f"---→ Args=%s"%(Args,), file=sys.stderr)

  if Options.verbose:
    print(f"%s starting at %s"%(Options.PrgName, datetime.now(),))
  
  Options.tmpdir = os.environ.get("TMPDIR", default=Options.tmp)
  if Options.DEBUG:
    print(f"%s using '%s' for temporary files"%(Options.PrgName, Options.tmpdir,))
  
  if Options.dolog:
    if Options.logfile:
      pass
    else:
      Options.logfile=log_filename()
      if Options.DEBUG:
        print(f"%s: Options.logfile='%s'"%(Options.PrgName, Options.logfile, ))
  log = set_logging()

  if len(Args)!=1:
    log.critical(f"%s: no configuration file given, exiting ..."%(Options.PrgName,))
    sys.exit(1)
  else:
    confdb = Args[0]
    log.info(f"%s: starting execution"%(Options.PrgName,))
    log.info(f"%s: configuration file '%s'"%(Options.PrgName, confdb,))
    log.info(f"%s: will check for files to transmit each %d %s"%(Options.PrgName, Options.wait, "seconds" if Options.seconds else "minutes",))
    if not Options.connection:
      log.critical(f"%s: Connection selector not given, exiting..."%(Options.PrgName, ))
      sys.exit(2)
    if not os.path.isfile(confdb):
      log.critical(f"%s: Could not use '%s', exiting..."%(Options.PrgName, confdb,))
      sys.exit(3)
    conf = load_all_conf(confdb)
    if Options.DEBUG:
      log.debug(f"%s: conf='%s'"%(Options.PrgName, conf,))
    # Re-set logging
    Options.logfile = log_filename(conf[0])
    log.info(f"%s changing to new log file '%s'"%(Options.PrgName, Options.logfile,))
    log = None
    log = set_logging(add_screen=False)
    log.info(f"%s changed to new log file '%s'"%(Options.PrgName, Options.logfile,))
    
    wait = Options.wait if Options.seconds else 60*Options.wait
    if Options.DEBUG:
      unit = "s" if Options.seconds else "m"
      log.debug(f"---→ Waiting for %s%s"%(Options.wait, unit,))
    while True:
      if Options.DEBUG:
        print("\n")
      print(f"%s"%(datetime.now(),))
      transmit_all(conf[0], conf[1])
      receive_all(conf[0], conf[2])
      sleep(wait)
except KeyboardInterrupt:
  log.critical(f"%s: Process cancelled!"%(Options.PrgName,))
  sys.stderr.flush()
  sys.exit(1)
