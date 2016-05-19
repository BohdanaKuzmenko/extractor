from unittest import TestCase
from app.services.check_bios.text_normalizer import *


class TestTextNormalizer(TestCase):
    def test_sentences_splitter(self):
        text = "First sent. The second one."
        self.assertEqual(sentences_splitter(text), [(1, 'First sent.'), (2, 'The second one.')])

    def test_remove_punctuation(self):
        text = "a, b. c!, d."
        self.assertEqual(remove_punctuation(text), " a b c d ")

    def test_remove_content(self):
        text = "some text important info and unneeded data."
        to_remove = " and unneeded data"
        self.assertEqual(remove_content(text, to_remove), "some text important info .")
