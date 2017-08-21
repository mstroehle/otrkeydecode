import subprocess
import ftplib
from datetime import datetime, timedelta
import os
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
        if val is None:
            return default
        else:
            return to_type(val)
        
    except (ValueError, TypeError):
        return default

stopsignal = False

def handler_stop_signals(signum, frame):
    global stopsignal
    stopsignal = True

signal.signal(signal.SIGINT, handler_stop_signals)
signal.signal(signal.SIGTERM, handler_stop_signals)


""" Logging Configuration """    
loglevel = safe_cast(os.environ.get('LOG_LEVEL'), str, 'INFO')
formatter = logging.Formatter('%(asctime)s | %(name)s:%(lineno)d | %(funcName)s | %(levelname)s | %(message)s')

consolehandler = logging.StreamHandler(stdout)
consolehandler.setFormatter(formatter)
consolehandler.setLevel(loglevel)
    
logfilename = '/usr/log/otrkeydecoder.log'
filehandler = logging.handlers.RotatingFileHandler(logfilename, 1000000, 5)
filehandler.setFormatter(formatter)
filehandler.setLevel(loglevel)

log = logging.getLogger('otrkeydecode') 
log.setLevel(loglevel)
log.addHandler(consolehandler)
log.addHandler(filehandler)

""" Main configuration """
def config_module():

    config = {}

    config['source_path'] = '/usr/otrkey/'
    #config['destination_path'] = '/usr/video/'
    config['otrdecoder_executable'] = '/usr/otrdecoder/otrdecoder'

    config['otr_user'] = safe_cast(os.environ.get('OTR_USER'), str, 'x@y.z')
    config['otr_pass'] = safe_cast(os.environ.get('OTR_PASS'), str, 'supersecret')

    config['waitseconds'] = safe_cast(os.environ.get('DECODE_INTERVAL'),int, 3600)
    config['use_subfolders'] = safe_cast(os.environ.get('USE_DEST_SUBFOLDER'), bool, False)
    config['use_cutlists'] = safe_cast(os.environ.get('USE_CUTLIST'), bool, False)
    config['temp_path'] = '/tmp/'

    config['use_ftp'] = safe_cast(os.environ.get('USE_FTP'), bool, False)
    config['ftp_user'] = safe_cast(os.environ.get('FTP_USER'), str, 'x@y.z')
    config['ftp_pass'] = safe_cast(os.environ.get('FTP_PASS'), str, 'supersecret')
    config['ftp_server'] = safe_cast(os.environ.get('FTP_SERVER'), str, 'ftp.something.com')
    config['ftp_path'] = safe_cast(os.environ.get('FTP_PATH'), str, '/')
    
    return config


""" class otrkey logic """
class otrkey():
    """ class to handle otrkey files """

    def get_cutlist(self):
        log.debug('retrieve cutlist for {} file!'.format(self.source_file))

        try:
            if not self.use_cutlists:
                return None
            else:
                """ download list of cutlists into string """
                url = 'http://www.onlinetvrecorder.com/getcutlistini.php?filename=' + self.source_file
                response = urllib.request.urlopen(url)
                content = str(response.read().decode('utf-8'))
                log.debug(content)

                """ parse list of cutlists """
                cutlists = configparser.ConfigParser(strict=False, allow_no_value=True)
                cutlists.read_string(content)
    
                """ download the first cutlist to file in /tmp """
                url = cutlists['FILE1']['filename']
                cutlist_file = os.path.join(self.temp_path, os.path.basename(url))
                urllib.request.urlretrieve(url, cutlist_file)
            
                """ success """
                log.info('donloaded cutlist to {}...'.format(cutlist_file))
                return cutlist_file

        except:
            log.exception('Exception Traceback:')
            return None

    def get_subfolder(self):
        log.debug('retrieve output path for video....{}'.format(self.source_file))
    
        try:

            if not self.use_subfolders:
                return None
            
            else:
                fileparts = self.source_file.split('_')

                if os.path.exists(self.destination_path + fileparts[0]):
                    destful = fileparts[0] + '/'

                elif os.path.exists(self.destination_path + '_' + fileparts[0]):
                    destful = '_' + fileparts[0] + '/'

                elif self.source_file[0] in ['0', '1', '2', '3','4','5','6','7','8','9']:
                    destful = '_1-9/'

                elif self.source_file[0].upper() in ['I', 'J']:
                    destful = '_I-J/'

                elif self.source_file[0].upper() in ['N', 'O']:
                    destful = '_N-O/'

                elif self.source_file[0].upper() in ['P', 'Q']:
                    destful = '_P-Q/'
                        
                elif self.source_file[0].upper() in ['U', 'V', 'W', 'X', 'Y', 'Z']:
                    destful = '_U-Z/'

                else:
                    destful = '_' + self.source_file[0].upper() + '/'


                return destful

        except:
            log.exception('Exception Traceback:')
            return None

    def decode(self):
        """ decode file ------------------------------------------------------------"""
        
        if not self.decoded:
            log.debug('try to decode {} with cutlist {!s}'.format(self.source_fullpath, self.cutlist_fullpath))                    
    
            try:
               
                call = self.otrdecoder_executable + ' -i ' + self.source_fullpath + ' -o ' + self.temp_path + ' -e ' + self.otr_user + ' -p ' + self.otr_pass + ' -f'
                
                if not self.cutlist_fullpath is None:
                        call = call + ' -C ' + self.cutlist_fullpath
                
                log.debug('decode call: {} !'.format(call))

                process = subprocess.Popen(call, shell=True, stdout=subprocess.PIPE)
                process.wait()
        
                """ decoding successful ? """
                if process.returncode != 0:
                    log.error('decoding failed with code {!s} and output {!s}'.format(process.returncode, process.stdout.read()))
                    
                else:
                    log.info('Decoding succesfull with returncode {!s}.'.format(process.returncode))
                    self.decoded = True

            except:
                log.exception('Exception Traceback:')

    def move(self):
        """ move decoded videofile to ftp destination """    
        if not self.moved and self.decoded:
            log.debug('try to move {} to {}'.format(self.video_temp_fullpath, self.video_fullpath))

            """ login to ftp server """
            try:
                ftp = ftplib.FTP(host=self.ftp_server)
                ftp.login(user=self.ftp_user, passwd=self.ftp_pass)
            
            except ftplib.error_perm:
                log.debug('connection to ftp server failed: {}'.format(ftplib.error_perm))
                return


            """ check fpt_path exist ? """
            try:
                ftp.cwd(self.ftp_path)
            
            except ftplib.error_perm:
                log.debug('sourcepath not found: {}'.format(ftplib.error_perm))
                return

            """ make subfolder if not exists """
            if not self.video_subfolder is None:
                try:

                        items = []
                        ftp.retrlines('LIST', items.append ) 
                        items = map( str.split, items )
                        dirlist = [ item.pop() for item in items if item[0][0] == 'd' ]

                        if not (self.video_subfolder in dirlist):
                            ftp.mkd(self.video_subfolder)
                            log.debug("folder does not exitst, ftp.mkd: " + self.video_subfolder)
            
                        ftp.cwd(self.video_subfolder)

                except ftplib.error_perm:
                    log.debug('subfoulder not found/make: {}'.format(ftplib.error_perm))
                    return

            """ move file """
            try:
                ftp.storbinary('STOR ' + self.video_file, open(self.video_temp_fullpath, 'rb'))
            
            except ftplib.error_perm:
                log.debug('subfoulder not found/make: {}'.format(ftplib.error_perm))

            """ logout ftp session """
            ftp.quit()

            self.moved = True
 
    def __init__(self, otrkey_file, data):

        """ parse data dictionary into instance var """
        for key, value in data.items():
            if (not key in vars(self)):
                setattr(self, key, value)
        
        """ initiate instance members """
        self.source_file = otrkey_file
        self.source_fullpath = os.path.join(self.source_path, self.source_file)

        self.cutlist_fullpath = self.get_cutlist()

        self.video_subfolder = self.get_subfolder()
        """ self.video_path = os.path.join(self.destination_path, self.video_subfolder) """
        self.video_file = os.path.splitext(os.path.basename(self.source_file))[0]
        """ self.video_fullpath = os.path.join(self.video_path, self.video_file) """
        self.video_temp_fullpath = os.path.join(self.temp_path, self.video_file)
        
        self.decoded = False
        self.moved = False

        """ log otrkey data in debug mode """ 
        for key, value in vars(self).items():   
            log.debug('otrkey {} - member: {} = {!s}'.format(self.source_file, key, value))

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """ clean up files """
        if self.moved:       
            log.debug('cleanup {}'.format(self.source_file))
            try:

                if os.path.exists(self.cutlist_fullpath):
                    os.remove(self.cutlist_fullpath)

                if os.path.exists(self.video_temp_fullpath):
                    os.remove(self.video_temp_fullpath)

                if os.path.exists(self.source_fullpath):
                    os.remove(self.source_fullpath)

            except:
                log.exception('exception on {!s}'.format(__name__))

""" Main """
def main():
    log.info('otrkey decoder start main....')

    config = config_module()
    nextrun =  datetime.utcnow()

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
