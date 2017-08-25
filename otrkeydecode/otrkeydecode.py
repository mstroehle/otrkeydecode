import subprocess
import ftplib
from datetime import datetime, timedelta
import os, fnmatch
from sys import stdout, stderr

import logging
import logging.handlers

import signal
import urllib.request
import configparser
import re


""" helper """
def safe_cast(val, to_type, default=None):
    try:
        result = None
        if val is None:
            result = default
        else:
            if to_type is bool:
                result = str(val).lower() in ("yes", "true", "t", "1")
            else:
                result = to_type(val)
        return result
        
    except (ValueError, TypeError):
        return default

stopsignal = False

def handler_stop_signals(signum, frame):
    global stopsignal
    stopsignal = True

signal.signal(signal.SIGINT, handler_stop_signals)
signal.signal(signal.SIGTERM, handler_stop_signals)


""" Logging Configuration """
log = logging.getLogger('otrkeydecode')
def config_logger(log, loglevel):
    formatter = logging.Formatter('%(asctime)s | %(name)s:%(lineno)d | %(funcName)s | %(levelname)s | %(message)s')

    consolehandler = logging.StreamHandler(stdout)
    consolehandler.setFormatter(formatter)
    consolehandler.setLevel(loglevel)
    
    logfilename = '/usr/log/otrkeydecoder.log'
    filehandler = logging.handlers.RotatingFileHandler(logfilename, 10240, 5)
    filehandler.setFormatter(formatter)
    filehandler.setLevel(loglevel)

    log.setLevel(loglevel)
    log.addHandler(consolehandler)
    log.addHandler(filehandler)

""" Main configuration """
def config_module():

    config = {}

    config['source_path'] = '/usr/otrkey/'
    config['loglevel'] = safe_cast(os.environ.get('LOG_LEVEL'), str, 'INFO')
    config['otrdecoder_executable'] = 'otrdecoder'

    config['otr_user'] = safe_cast(os.environ.get('OTR_USER'), str, 'x@y.z')
    config['otr_pass'] = safe_cast(os.environ.get('OTR_PASS'), str, 'supersecret')

    config['waitseconds'] = safe_cast(os.environ.get('DECODE_INTERVAL'),int, 3600)
    config['use_subfolders'] = safe_cast(os.environ.get('USE_SUBFOLDERS'), bool, False)
    config['use_cutlists'] = safe_cast(os.environ.get('USE_CUTLIST'), bool, False)
    config['temp_path'] = '/tmp/'

    config['ftp_user'] = safe_cast(os.environ.get('FTP_USER'), str, 'x@y.z')
    config['ftp_pass'] = safe_cast(os.environ.get('FTP_PASS'), str, 'supersecret')
    config['ftp_server'] = safe_cast(os.environ.get('FTP_SERVER'), str, 'ftp.something.com')
    config['ftp_port'] = safe_cast(os.environ.get('FTP_PORT'), int, 21)
    config['ftp_path'] = safe_cast(os.environ.get('FTP_PATH'), str, '/')
    
    return config


""" class otrkey logic """
class otrkey():
    """ class to handle otrkey files """

    def get_cutlist(self):
        log.debug('retrieve cutlist for {} file!'.format(self.source_file))

        try:

            """ Is cutlist already in tmp folder ? """
            pattern = str(self.video_file).split('_TVOON_')[0] + '*.cutlist'
            log.debug('looking for existing cutlist with pattern: {}'.format(pattern))
            match = fnmatch.filter(os.listdir(self.temp_path), pattern)
            if match:
                cutlist_file = os.path.join(self.temp_path, match[0])
                return cutlist_file

            else:
                """ download list of cutlists into string """
                url = 'http://www.onlinetvrecorder.com/getcutlistini.php?filename=' + self.source_file
                response = urllib.request.urlopen(url)
                content = str(response.read().decode('utf-8', 'ignore'))

                """ parse list of cutlists """
                cutlists = configparser.ConfigParser(strict=False, allow_no_value=True)
                cutlists.read_string(content)
    
                """ download the first cutlist to file in /tmp """
                if cutlists.has_option('FILE1','filename'):
                    curlist_url = cutlists.get('FILE1','filename')
                    cutlist_file = os.path.join(self.temp_path, os.path.basename(curlist_url))
                    urllib.request.urlretrieve(curlist_url, cutlist_file)
            
                    """ success """
                    log.info('downloaded cutlist to {}...'.format(cutlist_file))
                    return cutlist_file
                else:
                    log.info('no cutlist for {} file!'.format(self.source_file))
                    return None

        except:
            log.exception('Exception Traceback:')
            return None

    def cwd_subfolder(self, ftp):
        """ change ftp folder to an subfolder if exists """
        log.debug('change ftp folder to an subfolder if exists...'.format(self.source_file))

        try:

            """ retrieve directories in ftp folder """
            items = []
            ftp.retrlines('LIST', items.append ) 
            items = map( str.split, items )
            dirlist = [ item.pop() for item in items if item[0][0] == 'd' ]

            fileparts = self.source_file.split('_')

            if ('_' + fileparts[0] in dirlist):
                ftp.cwd('_' + fileparts[0])
                return True
                
            if (fileparts[0] in dirlist):
                ftp.cwd(fileparts[0])
                return True

            if self.source_file[0] in ['0', '1', '2', '3','4','5','6','7','8','9']:
                subdir = '_1-9'

            elif self.source_file[0].upper() in ['I', 'J']:
                subdir = '_I-J'

            elif self.source_file[0].upper() in ['N', 'O']:
                subdir = '_N-O'

            elif self.source_file[0].upper() in ['P', 'Q']:
                subdir = '_P-Q'
                        
            elif self.source_file[0].upper() in ['U', 'V', 'W', 'X', 'Y', 'Z']:
                subdir = '_U-Z'

            else:
                subdir = '_' + self.source_file[0].upper()

            if (subdir not in dirlist):
                ftp.mkd(subdir)
                log.debug("folder does not exitst, ftp.mkd: " + self.video_subfolder)                            
                    
            ftp.cwd(subdir)
            return True

        except:
            log.exception('Exception Traceback:')
            return False

    def decode(self):
        """ decode file ------------------------------------------------------------"""
        
        if not self.decoded:
            log.debug('try to decode {} with cutlist {!s}'.format(self.source_fullpath, self.cutlist_fullpath))                    
    
            try:
               
                if os.path.exists(self.video_temp_fullpath):
                    log.info('Already decoded in former session: {!s}.'.format(self.video_temp_fullpath))
                    self.decoded = True
                
                else:
                    call = self.otrdecoder_executable + ' -i ' + self.source_fullpath + ' -o ' + self.temp_path + ' -e ' + self.otr_user + ' -p ' + self.otr_pass + ' -f'
                
                    if self.use_cutlists:
                        self.cutlist_fullpath = self.get_cutlist()
                        if not self.cutlist_fullpath is None:
                                call = call + ' -C ' + self.cutlist_fullpath
                
                    log.debug('decode call: {} !'.format(call))

                    process = subprocess.Popen(call, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                    process.wait()
        
                    """ decoding successful ? """
                    if process.returncode != 0:
                        log.error('decoding failed with code {!s} and output {!s}'.format(process.returncode, process.stderr.read()))
                    
                    else:
                        log.info('Decoding succesfull with returncode {!s}.'.format(process.returncode))
                        self.decoded = True

            except:
                log.exception('Exception Traceback:')

    def move(self):
        """ move decoded videofile to ftp destination """    
        if not self.moved and self.decoded:
            log.debug('try to move {} to ftp.//{}'.format(self.video_temp_fullpath, self.ftp_server))

            try:
                
                """ login to ftp server """
                ftp = ftplib.FTP
                if self.loglevel == 'DEBUG':
                    ftp.set_debuglevel = 2
                else:
                    ftp.set_debuglevel = 1

                ftp.connect(host=self.ftp_server, port=self.ftp_port)
                ftp.login(user=self.ftp_user, passwd=self.ftp_pass)
      
                """ check fpt_path exist ? """
                ftp.cwd(self.ftp_path)
            
                """ make subfolder if not exists """
                if self.cwd_subfolder(ftp):

                    """ move file """
                    ftp.storbinary('STOR ' + self.video_file, open(self.video_temp_fullpath, 'rb'))
                    self.moved = True
                    log.info('{} successfully moved to ftp {}'.format(self.video_file, self.ftp_server))

                """ logout ftp session """
                ftp.quit()
                
            except ftplib.all_errors as e:
                log.error('{!s}'.format(e))
 
    def __init__(self, otrkey_file, data):

        """ parse data dictionary into instance var """
        for key, value in data.items():
            if (not key in vars(self)):
                setattr(self, key, value)
        
        """ initiate instance members """
        self.source_file = otrkey_file
        self.source_fullpath = os.path.join(self.source_path, self.source_file)
        self.cutlist_fullpath = None
        self.video_file = os.path.splitext(os.path.basename(self.source_file))[0]
        self.video_temp_fullpath = os.path.join(self.temp_path, self.video_file)
        
        self.decoded = False
        self.moved = False

        log.info('operate {}'.format(self.video_file))

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """ clean up files """
        if self.moved:       
            log.debug('try cleanup {}'.format(self.source_file))
            try:

                if os.path.exists(self.cutlist_fullpath):
                    os.remove(self.cutlist_fullpath)

                if os.path.exists(self.video_temp_fullpath):
                    os.remove(self.video_temp_fullpath)

                if os.path.exists(self.source_fullpath):
                    os.remove(self.source_fullpath)

                log.info('cleanup successful for {}'.format(self.source_file))

            except:
                log.exception('exception on {!s}'.format(__name__))

""" Main """
def main():
    
    """ configuration """
    config = config_module()
    config_logger(log, config['loglevel'])
    nextrun =  datetime.utcnow()
    log.info('otrkey decoder start main....')
    
    """ log configuration in debug mode """
    if config['loglevel'] == 'DEBUG':
        for key, value in config.items():   
            log.debug('otrkeydecoder configuration: {} = {!s}'.format(key, value))

    """ run until stopsignal """
    while not stopsignal:

        if (datetime.utcnow() >= nextrun):

            """ loop all *.otrkey files in sourcefolder/volume  """ 
            log.info('run {!s}'.format(__name__))

            for file in os.listdir(config['source_path']): 
                if file.endswith(".otrkey"):
                    log.info('try...{!s}'.format(file))

                    with otrkey(file, config) as otrkey_file:
                        otrkey_file.decode()
                        otrkey_file.move()

            nextrun = datetime.utcnow() + timedelta(seconds=config['waitseconds'])
            log.info('next runtime in {!s} seconds at {!s}'.format(config['waitseconds'], nextrun))

    """ goodby """ 
    log.info('otrkey decoder main terminated. Goodby!')

""" run main if not imported """
if __name__ == '__main__':
    main()
