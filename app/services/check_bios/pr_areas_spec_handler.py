from app.services.check_bios.text_normalizer import *
from pandas import DataFrame

STOP_WORDS = ["prior to", "before joining"]


def get_suitable_regex(sentence, regexes):
    for regex in regexes:
        # print(regex)
        pattern = re.compile(regex)
        if re.search(pattern, sentence):
            return (regex, str(regexes.index(regex) + 1))
    return []


def extract_sentences(full_bio, regexes, limit=None):
    result = []
    sentences = sentences_splitter(full_bio)
    for sentence in sentences:
        regex_info = get_suitable_regex(sentence, regexes)
        if regex_info:
            regex, regexes_index = regex_info
            if [word not in sentence.lower() for word in STOP_WORDS]:
                result.append((sentences.index(sentence) + 1, sentence, regex, regexes_index))
    return result


if __name__ == '__main__':
    pass
