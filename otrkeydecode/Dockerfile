FROM python:3.5-alpine
MAINTAINER oliver@app-workshop.de

# ARGs & ENVs
# -------------------------------------------------------
ENV OTR_DECODER otrdecoder-bin-64bit-linux-static-v519
ENV OTR_USER xy@z.com
ENV OTR_PASS supersecret
ENV LOG_LEVEL DEBUG
ENV DECODE_INTERVAL 600
ENV USE_DEST_SUBFOLDER True
ENV USE_CUTLIST True
ENV FTP_SERVER ftp.something.com
ENV FTP_PORT 21
ENV FTP_USER xyz
ENV FTP_PASS supersecret
ENV FTP_PATH /

# Decoder
# http://www.onlinetvrecorder.com/downloads/otrdecoder-bin-32bit-linux-static-v519.tar.bz2
# http://www.onlinetvrecorder.com/downloads/otrdecoder-bin-64bit-linux-static-v519.tar.bz2
# Easydecoder
# http://www.onlinetvrecorder.com/downloads/qotrdecoder-linux-Ubuntu_8.04-i686-0.0.248-r1133.tar.bz2
# http://www.onlinetvrecorder.com/downloads/otrdecoder-bin-x86_64-unknown-linux-gnu-0.4.1133.tar.bz2
# otrdecoder-bin-i686-pc-linux-gnu-static-0.4.995

# dirs & volumes
# --------------------------------------------------------
RUN mkdir -p /usr/src \
	&& mkdir -p /usr/otrkey \
	#&& mkdir -p /usr/video \
	&& mkdir -p /usr/otrdecoder \
	&& mkdir -p /usr/log

# download otrdecoder
# ----------------------------------------------------------
RUN wget -P /tmp http://www.onlinetvrecorder.com/downloads/$OTR_DECODER.tar.bz2 \
    && tar -zxjf /tmp/$OTR_DECODER.tar.bz2 -C /tmp \
	&& mv /tmp/$OTR_DECODER/* /usr/otrdecoder/ \
	&& cd /usr/otrdecoder && ls -al \
	&& rm -rf /tmp/* \
	&& cd /tmp && ls -al

# install otrdecoder / symlinks
# -----------------------------------------------------------------------------------
RUN mkdir -p ~/bin \
	&& export PATH=$PATH:~/bin \
	&& ln -sf /otr/otrdecoder/otrdecoder ~/bin/otrdecoder \
	&& ln -sf /otr/otrdecoder/otrdecoder /usr/bin/otrdecoder

# copy source
# ------------------------------------------------------------
WORKDIR /usr/src
COPY . /usr/src/

# Install requirements
# ----------------------------------------------------------
RUN pip install --no-cache-dir -r /usr/src/requirements.txt

# volumes
VOLUME /usr/src/
VOLUME /usr/log/
VOLUME /usr/otrkey/
#VOLUME /usr/video/
VOLUME /tmp/

# Start/Stop
STOPSIGNAL SIGTERM
ENTRYPOINT ["python3", "otrkeydecode.py"]
