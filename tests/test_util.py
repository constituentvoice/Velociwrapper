from __future__ import absolute_import
import types
from .setup import VWTestSetup
from velociwrapper.util import *
from velociwrapper.base import VWBase

class TestUtil(VWTestSetup):
    def test_all_subclasses(self):
        gen = all_subclasses(VWBase)
        self.assertIsInstance(gen, types.GeneratorType)

        test = [x.__name__ for x in gen]

        self.assertTrue('TestModel' in test)

    def test_unset(self):
        self.assertFalse(bool(unset))

    def test_dialect(self):
        self.assertIsInstance(VWDialect.dialect(), int)
