# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (C) 2017, Ryan P. Wilson
#
#      Authority FX, Inc.
#      www.authorityfx.com

import math
from afxthreads.core import MultiProcessor


class Bounds(object):
    """Class to handle image bounding coordinates.

    Args:
        x1 (int): Lower left x coordinate
        y1 (int): Lower left y coordinate
        x2 (int): Upper right x coordinate
        y2 (int): Upper right y coordinate

    Note:
        Pixel coordinates are inclusive.

    """
    def __init__(self, x1 = 0, y1 = 0, x2 = 0, y2 = 0):
        self.set_bounds(x1, y1, x2, y2)

    @property
    def x1(self):
        return self._x1
    @property
    def x2(self):
        return self._x2
    @property
    def y1(self):
        return self._y1
    @property
    def y2(self):
        return self._y2

    @x1.setter
    def x1(self, x1):
        self._x1 = int(x1)
    @x2.setter
    def x2(self, x2):
        self._x2 = int(x2)
    @y1.setter
    def y1(self, y1):
        self._y1 = int(y1)
    @y2.setter
    def y2(self, y2):
        self._y2 = int(y2)

    def set_bounds(self, x1, y1, x2, y2):
        self._x1 = int(x1)
        self._x2 = int(x2)
        self._y1 = int(y1)
        self._y2 = int(y2)

    def pad_bounds(self, x, y):
        """Expand bounding region of lower left and upper right by x and y"""
        self._x1 -= int(x)
        self._y1 -= int(y)
        self._x2 += int(x)
        self._y2 += int(y)

    def width(self):
        return self._x2 - self._x1 + 1
    def height(self):
        return self._y2 - self._y1 + 1

    def check_bounds(self, x, y):
        """Returns True if x and y are within or equal to bounds."""
        return False if x < self.x or x > self._x2 or y < self.y or y > self._y2 else True
    def check_bounds_x(self, x):
        """Returns True if x is within or equal to bounds."""
        return False if x < self.x or x > self._x2 else True
    def check_bounds_y(self, y):
        """Returns True if y is within or equal to bounds."""
        return False if y < self.y or y > self._y2 else True

    def center(self):
        """Returns center x, y as tuple"""
        return self.center_x(), self.center_y()
    def center_x(self):
        """Returns x center as float."""
        return (self._x2 - self._x1) / 2.0 + self._x1
    def center_y(self):
        """Returns y center as float."""
        return (self._y2 - self._y1) / 2.0 + self._y1

    def clamp_x(self, x):
        """Returns x clamped to bounds."""
        return (x if x <= self._x2 else self._x2) if x >= self._x1 else self._x1
    def clamp_y(self, y):
        """Returns y clamped to bounds."""
        return (y if y <= self._y2 else self._y2) if y >= self._y1 else self._y1


class ImageMultiProcessor(MultiProcessor):
    """Child class of MultiThreader with additional methods to process images in
    smaller parallel chunks.
    """

    def process_by_rows(self, f, args=(Bounds(),), kwargs={}, exc_traceback=False, exc_callback=None):
        """"Add work to process pool for each row in bounds argument

        Note:
            The first argument of f is required to be a Bounds object.

        Args:
            f (callable): A callable to be called by a worker process.
            args (tuple): Defaults to (Bounds()). Arguments to be passed to
                f when called.
            kwargs (dict): Defaults to empty dictionary. Arbitrary keyword
                arguments passed to f.
            exc_callback(callable): Called when exception is handled by worker.

        """
        region = args[0]
        for row in range(region.y1, region.y2 + 1):
            row_region = Bounds(region.x1, row, region.x2, row)
            self.add_work(f, (row_region,) + args[1:], kwargs, exc_traceback, exc_callback)

    def process_by_chunks(self, f, args=(Bounds(),), kwargs={}, exc_traceback=False, exc_callback=None):
        """Spit bounds argument into equal chunks and add to process pool

        Note:
            The first argument of f is required to be a Bounds object.
        Args:
            f (callable): A callable to be called by a worker process.
            args (tuple): Defaults to (Bounds()). Arguments to be passed to
                  f when called.
            kwargs (dict): Defaults to empty dictionary. Arbitrary keyword
                arguments passed to f.
            exc_callback(callable): Called when exception is handled by worker.

        """
        region = args[0]
        num_chunks = min(2 * self.processes(), region.height())
        for i in range(0, num_chunks):
            y1 = math.ceil(region.height() * i / float(num_chunks)) + region.y1
            y2 = math.ceil(region.height() * (i + 1) / float(num_chunks)) - 1  + region.y1
            chunk_region = Bounds(region.x1, y1, region.x2, y2)
            self.add_work(f, (chunk_region,) + args[1:], kwargs, exc_traceback, exc_callback)