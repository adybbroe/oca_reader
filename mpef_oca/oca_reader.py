#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2016 Adam.Dybbroe

# Author(s):

#   Adam.Dybbroe <a000680@c20671.ad.smhi.se>

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

"""Reader for the OCA products
"""

"""
How to gather the LRIT files and skip the header:

for file in `ls /disk2/testdata/OCA/L-000-MSG3__-MPEF________-OCAE_____-0000??___-*-__`;do echo $file; dd if=$file bs=1c skip=103 >> tmp;done

"""

import os
import pygrib
import numpy as np
import os.path
from glob import glob
import tempfile
import pyresample as pr
from trollsift import parser
from mpop.imageo import geo_image
from mpop.imageo import palettes


CFG_DIR = os.environ.get('MPEF_OCA_CONFIG_DIR', './')
AREA_DEF_FILE = os.path.join(CFG_DIR, "areas.def")
if not os.path.exists(AREA_DEF_FILE):
    raise IOError('Config file %s does not exist!' % AREA_DEF_FILE)


LRIT_PATTERN = "L-000-{platform_name:_<5s}_-MPEF________-OCAE_____-{segment:_<9s}-{nominal_time:%Y%m%d%H%M}-{compressed:_<2s}"

from .utils import (SCENE_TYPE_LAYERS, OCA_FIELDS, FIELDNAMES,
                    get_reff_legend,
                    get_cot_legend,
                    get_scenetype_legend,
                    get_ctp_legend)


palette_func = {'ll_ctp': get_ctp_legend,
                'ul_ctp': get_ctp_legend,
                'ul_cot': get_cot_legend,
                'll_cot': get_cot_legend,
                'reff': get_reff_legend,
                'scenetype': get_scenetype_legend}


class Grib(object):

    def __init__(self, fname):

        self._abspath = os.path.abspath(fname)

    @property
    def nmsgs(self):
        '''Number of GRIB messages in file.
        '''

        prop = 'nmsgs'
        attr = '_{}'.format(prop)

        if not hasattr(self, attr):
            grbs = pygrib.open(self._abspath)
            nmsgs = grbs.messages
            grbs.close()

            setattr(self, attr, nmsgs)

        return getattr(self, attr)

    def get(self, gmessage, key='values'):
        '''
        Returns the value for the 'key' for a given message number 'gmessage' or
        message field name 'gmessage'.
        '''

        grbs = pygrib.open(self._abspath)

        if type(gmessage) == int:
            mnbr = gmessage
        elif type(gmessage) == str:
            msg_found = False
            msgnum = 1
            while msgnum < self.nmsgs + 1:
                if grbs[msgnum]['parameterName'] == gmessage:
                    msg_found = True
                    break
                msgnum = msgnum + 1

            if msg_found:
                mnbr = msgnum
            else:
                print("No Grib message found with parameter name = %s" %
                      gmessage)
                return None

        if grbs[mnbr].valid_key(key):

            arr = grbs[mnbr][key]
            grbs.close()
            return arr
        else:
            grbs.close()
            return


class OCAField(object):

    """One OCA data field with metadata"""

    def __init__(self, units=None, longname='', shortname=''):
        self.units = units
        self.data = None
        self.error = None
        self.longname = None
        self.shortname = None


class OCAData(object):

    """The OCA scene data"""

    def __init__(self):
        self._lritfiles = None
        self._gribfilename = None
        self._store_grib = False

        self.scenetype = OCAField()
        self.cost = OCAField()
        self.ul_cot = OCAField()
        self.ll_cot = OCAField()
        self.ul_ctp = OCAField()
        self.ll_ctp = OCAField()
        self.reff = OCAField()
        self._projectables = []
        for field in FIELDNAMES.keys():
            self._projectables.append(field)

        self.timeslot = None
        self.area_def = pr.utils.load_area(AREA_DEF_FILE, 'met09globeFull')

    def readgrib(self):
        """Read the data"""

        oca = Grib(self._gribfilename)
        self.scenetype.data = oca.get('Pixel scene type')[::-1, ::-1]
        self.scenetype.longname = OCA_FIELDS[0]['Pixel scene type']

        for field in FIELDNAMES.keys():

            setattr(getattr(self, field), 'data', oca.get(
                FIELDNAMES[field][0])[::-1, ::-1])
            param = [s for s in OCA_FIELDS if FIELDNAMES[field][0] in s][0]
            if 'units' in param:
                setattr(getattr(self, field), 'units', param['units'])
            if 'abbrev' in param:
                setattr(getattr(self, field), 'shortname', param['abbrev'])
            setattr(getattr(self, field), 'longname',
                    param[FIELDNAMES[field][0]])
            param_name = FIELDNAMES[field][1]
            if param_name:
                setattr(
                    getattr(self, field), 'error', oca.get(param_name)[::-1, ::-1])

        if not self._store_grib:
            os.remove(self._gribfilename)

    def read_from_lrit(self, filenames, gribfilename=None):
        """Read and concatenate the LRIT segments"""

        self._lritfiles = filenames

        if len(filenames) == 0:
            print("No files provided!")
            return

        if gribfilename:
            self._store_grib = True
            self._gribfilename = gribfilename
        else:
            self._store_grib = False
            self._gribfilename = tempfile.mktemp(suffix='.grb')

        p__ = parser.Parser(LRIT_PATTERN)

        bstr = {}
        nsegments = 0
        for lritfile in self._lritfiles:
            if os.path.basename(lritfile).find('PRO') > 0:
                print("PRO file... %s: Skip it..." % lritfile)
                continue

            res = p__.parse(os.path.basename(lritfile))
            segm = int(res['segment'].strip('_'))
            if not self.timeslot:
                self.timeslot = res['nominal_time']
            print("Segment = %d" % segm)
            nsegments = nsegments + 1

            with open(lritfile) as fpt:
                fpt.seek(103)
                bstr[segm] = fpt.read()

        fstr = bstr[1]
        for idx in range(2, nsegments + 1):
            fstr = fstr + bstr[idx]

        with open(self._gribfilename, 'wb') as fpt:
            fpt.write(fstr)

        self.readgrib()

    def project(self, areaid):
        """Project the data"""

        lons, lats = self.area_def.get_lonlats()
        lons = np.ma.masked_outside(lons, -180, 180)
        lats = np.ma.masked_outside(lats, -90, 90)

        swath_def = pr.geometry.SwathDefinition(lons, lats)

        out_area_def = pr.utils.load_area(AREA_DEF_FILE, areaid)

        for item in self._projectables:
            data = getattr(getattr(self, item), 'data')
            result = pr.kd_tree.resample_nearest(swath_def, data, out_area_def,
                                                 radius_of_influence=20000,
                                                 fill_value=None)
            setattr(getattr(self, item), 'data', result)

        self.area_def = out_area_def

    def make_image(self, fieldname):
        """Make an mpop GeoImage image of the oca parameter 'fieldname'"""

        palette = palette_func[fieldname]()
        data = getattr(getattr(self, fieldname), 'data')
        if fieldname in ['ul_ctp', 'll_ctp']:
            data = (22. - data / 5000.).astype('Int16')

        elif fieldname in ['reff']:
            data = (data * 1000000. + 0.5).astype('uint8')

        img = geo_image.GeoImage(data, self.area_def.area_id,
                                 self.timeslot, fill_value=(0), mode="P",
                                 palette=palette)
        return img
