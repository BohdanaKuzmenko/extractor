from app.services.check_bios.text_normalizer import *
from pandas import DataFrame, set_option

STOP_WORDS = ["prior to", "before joining"]


def get_suitable_regex(sentence, regexes):
    for regex in regexes:
        pattern = re.compile(regex)
        if re.search(pattern, sentence):
            suitable = (regex, str(regexes.index(regex) + 1))
            return suitable
    return []


def extract_sentences(link, full_bio, regexes):
    set_option('display.max_colwidth', -1)
    sentences = sentences_splitter(full_bio)
    suitable_regexes = []
    sentences_df = DataFrame(sentences, columns=['sentence'])
    for regex in regexes:
        if not sentences_df[sentences_df.sentence.str.contains(regex)].empty:
            suitable_regexes.append(regex)
    if suitable_regexes:
        data_frame = DataFrame([link], columns=["profileUrl"])
        data_frame["full_bio"] = DataFrame([full_bio]).values
        data_frame["regex"] = DataFrame(['; '.join(suitable_regexes)]).values
        return (data_frame)


if __name__ == '__main__':
    pass
