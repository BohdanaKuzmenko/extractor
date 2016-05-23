import nltk
nltk.download('punkt')
from nltk import sent_tokenize
import re


def sentences_splitter(raw_text):
    all_sentences = sent_tokenize(raw_text)
    unique_sentences = []
    [unique_sentences.append(sentence) for sentence in all_sentences if sentence not in unique_sentences ]
    return [(sentence_index+1, unique_sentences[sentence_index]) for sentence_index in range(len(unique_sentences))]


def remove_punctuation(value):
    return ' ' + ' '.join(re.findall("[\w]+", ' '.join(value.lower().replace('&', ' and').split()))) + ' '


def remove_content(sentence, value):
    return sentence.replace(value, " ")


if __name__ == '__main__':
    pass
