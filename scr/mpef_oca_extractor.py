#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2016 Adam.Dybbroe

# Author(s):

#   Adam.Dybbroe <adam.dybbroe@smhi.se>

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

"""A posttroll runner to read MPEF OCA SEVIRI cloud products and make netCDF
output and imagery to users

"""


import os
from ConfigParser import RawConfigParser
import logging
LOG = logging.getLogger(__name__)

CFG_DIR = os.environ.get('MPEF_OCA_CONFIG_DIR', './')
DIST = os.environ.get("SMHI_DIST", 'elin4')
if not DIST or DIST == 'linda4':
    MODE = 'offline'
else:
    MODE = os.environ.get("SMHI_MODE", 'offline')

CONF = RawConfigParser()
CFG_FILE = os.path.join(CFG_DIR, "mpef_oca_config.cfg")
LOG.debug("Config file = " + str(CFG_FILE))
AREA_DEF_FILE = os.path.join(CFG_DIR, "areas.def")
if not os.path.exists(CFG_FILE):
    raise IOError('Config file %s does not exist!' % CFG_FILE)

CONF.read(CFG_FILE)

OPTIONS = {}
for option, value in CONF.items("DEFAULT"):
    OPTIONS[option] = value

for option, value in CONF.items(MODE):
    OPTIONS[option] = value

OUTPUT_PATH = OPTIONS['output_path']
#: Default time format
_DEFAULT_TIME_FORMAT = '%Y-%m-%d %H:%M:%S'

#: Default log format
_DEFAULT_LOG_FORMAT = '[%(levelname)s: %(asctime)s : %(name)s] %(message)s'

servername = None
import socket
servername = socket.gethostname()
SERVERNAME = OPTIONS.get('servername', servername)

import sys
from urlparse import urlparse
import posttroll.subscriber
from posttroll.publisher import Publish
import netifaces
from posttroll.message import Message
from datetime import datetime

from multiprocessing import Pool, Manager
import threading
from Queue import Empty

SATELLITE = {'MSG3': 'Meteosat-10',
             'MSG2': 'Meteosat-09',
             'MSG1': 'Meteosat-08',
             'MSG4': 'Meteosat-11',
             }

SUPPORTED_SATELLITES = ['Meteosat-08', 'Meteosat-09',
                        'Meteosat-10', 'Meteosat-11']


def get_local_ips():
    inet_addrs = [netifaces.ifaddresses(iface).get(netifaces.AF_INET)
                  for iface in netifaces.interfaces()]
    ips = []
    for addr in inet_addrs:
        if addr is not None:
            for add in addr:
                ips.append(add['addr'])
    return ips


def reset_job_registry(objdict, key):
    """Remove job key from registry"""
    LOG.debug("Release/reset job-key " + str(key) + " from job registry")
    if key in objdict:
        objdict.pop(key)
    else:
        LOG.warning("Nothing to reset/release - " +
                    "Register didn't contain any entry matching: " +
                    str(key))
    return


class FilePublisher(threading.Thread):

    """A publisher for the oca level2 netCDF files. Picks up the return value from
    the oca_extractor when ready, and publishes the files via posttroll

    """

    def __init__(self, queue):
        threading.Thread.__init__(self)
        self.loop = True
        self.queue = queue
        self.jobs = {}

    def stop(self):
        """Stops the file publisher"""
        self.loop = False
        self.queue.put(None)

    def run(self):

        with Publish('mpef_oca_extractor', 0, ['netCDF/3', ]) as publisher:

            while self.loop:
                retv = self.queue.get()

                if retv != None:
                    LOG.info("Publish the OCA level-2 netcdf file")
                    publisher.send(retv)


class FileListener(threading.Thread):

    """A file listener class, to listen for incoming messages with a 
    relevant file for further processing"""

    def __init__(self, queue):
        threading.Thread.__init__(self)
        self.loop = True
        self.queue = queue

    def stop(self):
        """Stops the file listener"""
        self.loop = False
        self.queue.put(None)

    def run(self):

        with posttroll.subscriber.Subscribe('', [OPTIONS['posttroll_topic'], ],
                                            True) as subscr:

            for msg in subscr.recv(timeout=90):
                if not self.loop:
                    break

                # Check if it is a relevant message:
                if self.check_message(msg):
                    LOG.debug("Put the message on the queue...")
                    self.queue.put(msg)

    def check_message(self, msg):

        if not msg:
            return False

        urlobj = urlparse(msg.data['uri'])
        server = urlobj.netloc
        url_ip = socket.gethostbyname(urlobj.netloc)
        if urlobj.netloc and (url_ip not in get_local_ips()):
            LOG.warning("Server %s not the current one: %s",
                        str(server),
                        socket.gethostname())
            return False

        if ('platform_name' not in msg.data or
                'start_time' not in msg.data):
            LOG.warning(
                "Message is lacking crucial fields...")
            return False

        LOG.debug("Ok: message = %s", str(msg))
        return True


def create_message(resultfile, mda):
    """Create the posttroll message"""

    to_send = mda.copy()
    to_send['uri'] = ('ssh://%s/%s' % (SERVERNAME, resultfile))
    to_send['uid'] = resultfile
    to_send['type'] = 'netCDF'
    to_send['format'] = 'OCA'
    to_send['data_processing_level'] = '3'
    environment = MODE
    pub_message = Message('/' + to_send['format'] + '/' +
                          to_send['data_processing_level'] +
                          environment +
                          '/0deg/regional/',
                          "file", to_send).encode()

    return pub_message


def oca_extractor(mda, scene, job_id, publish_q):
    """Read the LRIT encoded Grib files and convert to netCDF

    """

    from mpef_oca import oca_reader

    try:
        LOG.debug("OCA data reader: Start...")

        lrit_files = scene['filenames']
        for lritfile in lrit_files:
            LOG.info("LRIT file = %s", lritfile)

        oca = oca_reader.OCAData()
        oca.read_from_lrit(lrit_files)

        # print("Project...")
        # this.project('euron1')
        # print("Projection done...")

        # img = this.make_image('reff')
        # #img = this.make_image('ul_ctp')
        # img.add_overlay()
        # img.show()
        # #img.save('./ul_ctp_%s.png' % this.timeslot.strftime('%Y%m%d%H%M'))

    except:
        LOG.exception('Failed in oca_extractor...')
        return


def ready2run(msg, files4oca, job_register, sceneid):
    """Check whether we have all input and are ready to run """

    from trollduction.producer import check_uri

    LOG.debug("Ready to run...")
    LOG.info("Got message: " + str(msg))

    uris = []
    satid = SATELLITE.get(msg.data['platform_name'], msg.data['platform_name'])
    if (msg.type == 'dataset' and satid in SUPPORTED_SATELLITES):
        LOG.info('Dataset: ' + str(msg.data['dataset']))
        LOG.info('Got a dataset on the satellite %s', satid)
        LOG.info(
            '\t ...thus we can assume we have everything we need for the OCA product')
        for obj in msg.data['dataset']:
            uris.append(obj['uri'])
    else:
        LOG.debug(
            "Ignoring this type of message data: type = " + str(msg.type))
        return False

    try:
        level1_files = check_uri(uris)
    except IOError:
        LOG.warning('One or more files not present on this host!')
        return False

    if sceneid not in files4oca:
        files4oca[sceneid] = []

    for item in level1_files:
        fname = os.path.basename(item)
        files4oca[sceneid].append(fname)

    LOG.debug("files4oca: %s", str(files4oca[sceneid]))

    job_register[sceneid] = datetime.utcnow()
    return True


def oca_runner():
    """Listens and triggers processing"""

    LOG.info(
        "*** Start the extraction and conversion of MPEF OCA level2 profiles")

    pool = Pool(processes=6, maxtasksperchild=1)
    manager = Manager()
    listener_q = manager.Queue()
    publisher_q = manager.Queue()

    pub_thread = FilePublisher(publisher_q)
    pub_thread.start()
    listen_thread = FileListener(listener_q)
    listen_thread.start()

    files4oca = {}
    jobs_dict = {}
    while True:

        try:
            msg = listener_q.get()
        except Empty:
            LOG.debug("Empty listener queue...")
            continue

        LOG.debug(
            "Number of threads currently alive: " + str(threading.active_count()))

        if 'start_time' in msg.data:
            start_time = msg.data['start_time']
        else:
            LOG.warning("No start_time in message!")
            start_time = None

        sensor = str(msg.data['sensor'])
        platform_name = SATELLITE.get(msg.data['platform_name'],
                                      msg.data['platform_name'])

        keyname = (str(platform_name) + '_' +
                   str(start_time.strftime('%Y%m%d%H%M')))

        status = ready2run(msg, files4oca,
                           jobs_dict, keyname)

        if status:
            jobs_dict[keyname] = datetime.utcnow()

            urlobj = urlparse(msg.data['uri'])
            path, fname = os.path.split(urlobj.path)
            LOG.debug("path " + str(path) + " filename = " + str(fname))

            scene = {'platform_name': platform_name,
                     'starttime': start_time,
                     'sensor': sensor,
                     'filenames': files4oca[keyname]}

            if keyname not in jobs_dict:
                LOG.warning("Scene-run seems unregistered! Forget it...")
                continue

            pool.apply_async(oca_extractor,
                             (msg.data, scene,
                              jobs_dict[
                                  keyname],
                              publisher_q))

            # Clean the files4oca dict:
            LOG.debug("files4oca: " + str(files4oca))
            try:
                files4oca.pop(keyname)
            except KeyError:
                LOG.warning("Failed trying to remove key " + str(keyname) +
                            " from dictionary files4oca")
            LOG.debug("After cleaning: files4oca = " + str(files4oca))

            # Block any future run on this scene for x minutes from now
            # x = 5
            thread_job_registry = threading.Timer(
                5 * 60.0, reset_job_registry, args=(jobs_dict, keyname))
            thread_job_registry.start()

    pool.close()
    pool.join()

    pub_thread.stop()
    listen_thread.stop()


if __name__ == "__main__":

    handler = logging.StreamHandler(sys.stderr)

    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter(fmt=_DEFAULT_LOG_FORMAT,
                                  datefmt=_DEFAULT_TIME_FORMAT)
    handler.setFormatter(formatter)
    logging.getLogger('').addHandler(handler)
    logging.getLogger('').setLevel(logging.DEBUG)
    logging.getLogger('posttroll').setLevel(logging.INFO)

    LOG = logging.getLogger('oca_reader')
    oca_runner()
