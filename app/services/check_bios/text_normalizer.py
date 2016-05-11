import nltk
import re


def sentences_splitter(raw_text):
    return nltk.sent_tokenize(raw_text)


def remove_punctuation(value):
    return ' ' + ' '.join(re.findall("[\w]+", ' '.join(value.lower().replace('&', ' and').split()))) + ' '


def remove_content(sentence, value):
    return sentence.replace(value, " ")


if __name__ == '__main__':
    pass
