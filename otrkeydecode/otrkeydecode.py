import subprocess
from datetime import datetime, timedelta
import os
from sys import stdout
import logging
import logging.handlers


""" Main configuration --------------------------------------------------------------------------------------------------
"""
def safe_cast(val, to_type, default=None):
    try:
        return to_type(val)
    except (ValueError, TypeError):
        return default

sourcepath = '/usr/otrkey/'
destpath = '/usr/video/'
otrdecoder = '/usr/otrdecoder/otrdecoder'

otruser = os.environ.get('OTR_USER')
otrpass = os.environ.get('OTR_PASS')

loglevel = os.environ.get('LOG_LEVEL')
waitseconds = safe_cast(os.environ.get('DECODE_INTERVAL'),int, 600)
subfolders = safe_cast(os.environ.get('USE_DEST_SUBFOLDER'), bool, False)
usecutlists = safe_cast(os.environ.get('USE_CUTLIST'), bool, True)

""" Logging Configuration ----------------------------------------------------------------------------------------------- 
"""    
formatter = logging.Formatter('%(asctime)s | %(name)s:%(lineno)d | %(funcName)s | %(levelname)s | %(message)s')

#console handler    
consolehandler = logging.StreamHandler(stdout)
consolehandler.setFormatter(formatter)
consolehandler.setLevel(loglevel)
    
#file handler
LOG_FILENAME = '/usr/log/otrkeydecoder.log'
filehandler = logging.handlers.RotatingFileHandler(LOG_FILENAME, 1000000, 5)
filehandler.setFormatter(formatter)
filehandler.setLevel(loglevel)

#diboards logger
log = logging.getLogger('otrkeydecoder') 
log.setLevel(loglevel)
log.addHandler(consolehandler)
log.addHandler(filehandler)

""" Handle SIGTERM, SIGINT -----------------------------------------------------------------------------------------------------
"""
import signal

stopsignal = False

def handler_stop_signals(signum, frame):
    global stopsignal
    stopsignal = True

signal.signal(signal.SIGINT, handler_stop_signals)
signal.signal(signal.SIGTERM, handler_stop_signals)


""" Main procedure -------------------------------------------------------------------------------------------------------
"""

def get_cutlist(file):

    import urllib.request
    import configparser

    try:

        """ download list of cutlists into string """
        url = 'http://www.onlinetvrecorder.com/getcutlistini.php?filename=' + file
        response = urllib.request.urlopen(url)
        content = str(response.read().decode('utf-8'))
        log.debug(content)

        """ parse list of cutlists """
        cutlists = configparser.ConfigParser(strict=False, allow_no_value=True)
        cutlists.read_string(content)
    
        """ download the first cutlist to file in sourcepath """
        url = cutlists['FILE1']['filename']
        cutlist_file = os.path.join(sourcepath, os.path.basename(url))
        urllib.request.urlretrieve(url,cutlist_file)
        log.info('donloaded cutlist to {}...'.format(cutlist_file))
        
        return cutlist_file

    except Exception as e:
        log.error(str(e.args))
        return None

def do_otrkeydecode():

    """ loop all *.otrkey files in sourcefolder/volume -------------------------------------------------------------
    """     
    for file in os.listdir(sourcepath):
                
        if file.endswith(".otrkey"):
            
            log.info('try...{!s}'.format(file))
            destful = ""
            sourceful = os.path.join(sourcepath, file)            
                    
            """ use subfolders ? -------------------------------------------------------------
            """
            log.debug('retrieve output path for decode....{}'.format(sourceful))
                    
            if not subfolders:
                    destful = destpath
            else:
                fileparts = file.split('_')

                if os.path.exists(destpath + fileparts[0]):
                    destful = destpath + fileparts[0] + '/'

                elif os.path.exists(destpath + '_' + fileparts[0]):
                    destful = destpath + '_' + fileparts[0] + '/'

                elif file[0] in ['0', '1', '2', '3','4','5','6','7','8','9']:
                    destful = destpath + '_1-9/'

                elif file[0].upper() in ['I', 'J']:
                    destful = destpath + '_I-J/'

                elif file[0].upper() in ['N', 'O']:
                    destful = destpath + '_N-O/'

                elif file[0].upper() in ['P', 'Q']:
                    destful = destpath + '_P-Q/'
                        
                elif file[0].upper() in ['U', 'V', 'W', 'X', 'Y', 'Z']:
                    destful = destpath + '_U-Z/'

                else:
                    destful = destpath + '_' + file[0].upper() + '/'


                if not os.path.exists(destful[:-1]):
                    os.mkdir(destful[:-1])

                log.info('output path for decode....{}'.format(destful))

                    
            """ decode file -------------------------------------------------------------
            """                    
                
            if os.path.exists(otrdecoder):
                
                call = otrdecoder + ' -i ' + sourceful + ' -o ' + destful + ' -e ' + otruser + ' -p ' + otrpass + ' -f'
                
                if usecutlists:
                    cutlist = get_cutlist(file,sourcepath)
                    if not cutlist is None:
                        call = call + ' -C ' + cutlist
                
                log.debug('decode call: {} !'.format(call))

                decode = subprocess.Popen(call, shell=True, stdout=subprocess.PIPE)
                decode.wait()
                log.info(decode.stdout.read())

                """ delete file if returncode == 0 -----------------------------------------------------------------------------------
                """
                log.debug('returncode: {!s}'.format(decode.returncode))
                if decode.returncode == 0:
                    log.debug('Decoding succesfull with returncode {!s}. Try to delete otrkey file and cutlist!'.format(decode.returncode))
                    os.remove(sourceful)
                    if not cutlist is None:
                        os.remove(cutlist)

            else:
                log.error('executeable not found: {}'.format(otrdecoder))

def main():
    log.info('otrkey decoder start main....')
    nextrun =  datetime.utcnow()

    while not stopsignal:

        if (datetime.utcnow() >= nextrun):
            do_otrkeydecode()
            nextrun = datetime.utcnow() + timedelta(seconds=waitseconds)
            log.info('next runtime in {!s} seconds at {!s}'.format(waitseconds, nextrun))

    """ goodby -------------------------------------------------------------
    """ 
    log.info('otrkey decoder main terminated. Goodby!')

if __name__ == '__main__':
    main()
