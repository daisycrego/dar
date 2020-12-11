#!/usr/bin/env python
"""
Wrapper script for executing all phonenumbers tests.
"""
import doctest
import unittest
import phonenumbers
from phonenumbers import util
from phonenumbers import re_util
from phonenumbers import unicode_util
from phonenumbers import geocoder
from phonenumbers import carrier
from phonenumbers import timezone
from tests import *


class DocTest(unittest.TestCase):
    def testDocStrings(self):
        self.assertEqual(0, doctest.testmod(phonenumbers)[0])
        self.assertEqual(0, doctest.testmod(util)[0])
        self.assertEqual(0, doctest.testmod(re_util)[0])
        self.assertEqual(0, doctest.testmod(unicode_util)[0])
        self.assertEqual(0, doctest.testmod(geocoder)[0])
        self.assertEqual(0, doctest.testmod(carrier)[0])
        self.assertEqual(0, doctest.testmod(timezone)[0])


if __name__ == '__main__':
    unittest.main()
