#!/usr/bin/env python3
"""The cases of fuzz/seed_corpus.py's held set (#713, #723): writes into a
scratch corpus under $TMPDIR, the committed fuzz/corpus untouched.

Run from the repository root: python3 -m unittest fuzz/seed_corpus_test.py
"""

import collections
import hashlib
import importlib.util
import os
import shutil
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
SPEC = importlib.util.spec_from_file_location("seed_corpus", os.path.join(HERE, "seed_corpus.py"))
seed_corpus = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(seed_corpus)

TARGET = "scratch"
ONE = b"body one"
TWO = b"body two"


def sha(data):
    return hashlib.sha1(data).digest()


class HeldSet(unittest.TestCase):
    def setUp(self):
        self.corpus = tempfile.mkdtemp(prefix="seed_corpus_test-")
        self.saved = (seed_corpus.CORPUS, seed_corpus.HELD)
        seed_corpus.CORPUS = self.corpus
        seed_corpus.HELD = {}

    def tearDown(self):
        seed_corpus.CORPUS, seed_corpus.HELD = self.saved
        shutil.rmtree(self.corpus)

    def on_disk(self):
        directory = os.path.join(self.corpus, TARGET)
        return [(n, open(os.path.join(directory, n), "rb").read()) for n in sorted(os.listdir(directory))]

    def test_an_overwrite_takes_the_old_digest_out(self):
        seed_corpus.write(TARGET, "a", ONE)
        seed_corpus.write(TARGET, "a", TWO)
        seed_corpus.write(TARGET, "b", ONE)
        self.assertEqual(self.on_disk(), [("a", TWO), ("b", ONE)])
        self.assertEqual(len(seed_corpus.HELD[TARGET]), 2)

        seed_corpus.write(TARGET, "c", TWO)
        self.assertEqual(self.on_disk(), [("a", TWO), ("b", ONE)])

        seed_corpus.write(TARGET, "a", TWO)
        self.assertEqual(self.on_disk(), [("a", TWO), ("b", ONE)])
        self.assertEqual(seed_corpus.HELD[TARGET], collections.Counter({sha(TWO): 1, sha(ONE): 1}))

    def test_the_set_is_read_from_the_files_on_disk(self):
        directory = os.path.join(self.corpus, TARGET)
        os.makedirs(directory)
        for name, data in (("x", ONE), ("y", ONE)):
            with open(os.path.join(directory, name), "wb") as f:
                f.write(data)
        seed_corpus.write(TARGET, "x", TWO)
        self.assertEqual(seed_corpus.HELD[TARGET], collections.Counter({sha(TWO): 1, sha(ONE): 1}))
        seed_corpus.write(TARGET, "z", ONE)
        self.assertEqual(self.on_disk(), [("x", TWO), ("y", ONE)])


if __name__ == "__main__":
    unittest.main()
