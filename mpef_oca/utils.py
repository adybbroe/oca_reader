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

"""Some helper functions and definitions for the OCA product reader and image generator
"""

import numpy as np
from mpop.imageo import palettes


SCENE_TYPE_LAYERS = {111: 'Single Layer Water Cloud',
                     112: 'Single Layer Ice Cloud',
                     113: 'Multi Layer Cloud'}

OCA_FIELDS = [{'Pixel scene type': 'Scene type'},
              {'24': 'Measurement Cost',
               'abbrev': 'JM', 'units': ''},
              {'25': 'Upper Layer Cloud Optical Thickness', 'units': '',
               'abbrev': 'ULCOT'},
              {'26': 'Upper Layer Cloud Top Pressure', 'units': 'Pa',
               'abbrev': 'ULCTP'},
              {'27': 'Upper Layer Cloud Effective Radius', 'units': 'm',
               'abbrev': 'ULCRE'},
              {'28': 'Error in Upper Layer Cloud Optical Thickness', 'units': '',
               'abbrev': 'ERR-ULCOT'},
              {'29': 'Error in Upper Layer Cloud Top Pressure', 'units': 'Pa',
               'abbrev': 'ERR-ULCTP'},
              {'30': 'Error in Upper Layer Cloud Effective Radius', 'units': 'm',
               'abbrev': 'ERR-ULCRE'},
              {'31': 'Lower Layer Cloud Optical Thickness',
                  'units': '', 'abbrev': 'LLCOT'},
              {'32': 'Lower Layer Cloud Top Pressure',
                  'units': 'Pa', 'abbrev': 'LLCTP'},
              {'33': 'Error in Lower Layer Cloud Optical Thickness',
                  'units': '', 'abbrev': 'ERR-LLCOT'},
              {'34': 'Error in Lower Layer Cloud Top Pressure', 'units': 'Pa',
               'abbrev': 'ERR-LLCTP'}]

FIELDNAMES = {'scenetype': ('Pixel scene type', None),
              'cost': ('24', None),
              'ul_cot': ('25', '28'),
              'ul_ctp': ('26', '29'),
              'reff': ('27', '30'),
              'll_cot': ('31', '33'),
              'll_ctp': ('32', '34')}


class LogColors(object):

    """
    Defines colors to use with `logdata2image`

    """

    def __init__(self, nodata, zeros, over, breaks):
        self.nodata = nodata
        self.zeros = zeros
        self.over = over
        self.breaks = breaks

    def palette(self, N=256):
        """
        Build a palette for logarithmic data images.

        """

        max_value = self.breaks[-1][0]

        palette = np.zeros((N, 3), dtype=np.uint8)

        b_last, rgb_last = self.breaks[0]
        for b, rgb in self.breaks[1:]:
            # Get a slice of the palette array for the current interval
            p = palette[
                np.log(b_last + 1) * N / np.log(max_value):np.log(b + 1) * N / np.log(max_value)]
            for i in range(3):  # red, green, blue
                p[:, i] = np.linspace(rgb_last[i], rgb[i], p.shape[0])
            b_last = b
            rgb_last = rgb

        palette[0] = self.nodata
        palette[1] = self.zeros
        palette[-1] = self.over

        return palette


class TriColors(LogColors):

    """
    Use three color tones in the intervals between the elements of *breaks*.

    """
    color_tones = [((0, 0, 200), (150, 150, 255)),  # dark to light blue
                   ((150, 150, 0), (255, 255, 8)),  # greyish to bright yellow
                   ((230, 150, 100), (230, 0, 0))]  # green to red

    nodata = (0, 0, 0)  # black
    # zeros = (20, 0, 20) # dark purple
    # black  #There is no need to mark zeros with another col
    zeros = (0, 0, 0)
    over = (255, 0, 0)  # bright red

    def __init__(self, breaks):
        breaks = [(breaks[0], TriColors.color_tones[0][0]),
                  (breaks[1], TriColors.color_tones[0][1]),

                  (breaks[1], TriColors.color_tones[1][0]),
                  (breaks[2], TriColors.color_tones[1][1]),

                  (breaks[2], TriColors.color_tones[2][0]),
                  (breaks[3], TriColors.color_tones[2][1])]

        LogColors.__init__(self, TriColors.nodata, TriColors.zeros,
                           TriColors.over, breaks)

CPP_COLORS = {'cpp_cot': TriColors([0, 3.6, 23, 700]),  # ISCCP intervals
              'cpp_reff': TriColors([0, 10, 20, 1000])}

CPP_COLORS['cot'] = CPP_COLORS['cpp_cot']
CPP_COLORS['reff'] = CPP_COLORS['cpp_reff']


def get_reff_legend():
    return get_log_legend('reff')


def get_cot_legend():
    return get_log_legend('cot')


def get_log_legend(product_name):
    # This is the same data as is used in logdata2image (when indata as for
    # the calls from cppimage)
    return CPP_COLORS[product_name].palette()


def get_scenetype_legend():

    # Colorize using PPS/CPP palette
    legend = np.array([[170, 130, 255],  # purple/blue for liquid (cph == 1)
                       [220, 200, 255],  # almost white for ice (cph == 2)
                       [255, 200, 200]   # Redish for multi layer clouds
                       ])
    legend = np.vstack([np.zeros((111, 3)), legend])
    palette = palettes.convert_palette(legend)
    return palette


def get_ctp_legend():
    """
    Get the Cloud Top Pressure color palette
    """

    legend = []
    legend.append((0, 0, 0))     # No data
    legend.append((255, 0, 216))  # 0: 1000-1050 hPa (=100000-105000 Pa)
    legend.append((126, 0, 43))  # 1: 950-1000 hPa
    legend.append((153, 20, 47))  # 2: 900-950 hPa
    legend.append((178, 51, 0))  # 3: 850-900 hPa
    legend.append((255, 76, 0))  # 4: 800-850 hPa
    legend.append((255, 102, 0))  # 5: 750-800 hPa
    legend.append((255, 164, 0))  # 6: 700-750 hPa
    legend.append((255, 216, 0))  # 7: 650-700 hPa
    legend.append((216, 255, 0))  # 8: 600-650 hPa
    legend.append((178, 255, 0))  # 9: 550-600 hPa
    legend.append((153, 255, 0))  # 10: 500-550 hPa
    legend.append((0, 255, 0))   # 11: 450-500 hPa
    legend.append((0, 140, 48))  # 12: 400-450 hPa
    legend.append((0, 178, 255))  # 13: 350-400 hPa
    legend.append((0, 216, 255))  # 14: 300-350 hPa
    legend.append((0, 255, 255))  # 15: 250-300 hPa
    legend.append((238, 214, 210))  # 16: 200-250 hPa
    legend.append((239, 239, 223))  # 17: 150-200 hPa
    legend.append((255, 255, 255))  # 18: 100-150 hPa
    legend.append((255, 255, 255))  # 19: 50-100 hPa
    legend.append((255, 255, 255))  # 20: 0-50 hPa  (=0-5000 Pa)

    palette = palettes.convert_palette(legend)
    return palette
