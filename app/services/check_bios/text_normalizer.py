import nltk
nltk.download('punkt')
from nltk import sent_tokenize
import re


def sentences_splitter(raw_text):
    sentences = sent_tokenize(raw_text)
    return [(sentence_index+1, sentences[sentence_index]) for sentence_index in range(len(sentences))]


def remove_punctuation(value):
    return ' ' + ' '.join(re.findall("[\w]+", ' '.join(value.lower().replace('&', ' and').split()))) + ' '


def remove_content(sentence, value):
    return sentence.replace(value, " ")


if __name__ == '__main__':
    pass
