from app.services.check_bios.text_normalizer import *
from app.services.check_bios.mapper import Mapper


class Predictor(object):
    @staticmethod
    def check_full_entry(full_default_list, full_predicted_list):
        full_matches = [value for value in full_default_list if value in full_predicted_list]
        return len(full_matches) == len(full_default_list)

    @staticmethod
    def check_equals(equals_default_list, equals_predicted_list):
        equals_matches = [value.strip() for value in equals_default_list if value in equals_predicted_list]
        return len(equals_matches) == len(equals_predicted_list) == len(equals_default_list)

    @staticmethod
    def get_predictions(features_file_name, sentence):
        predicted_areas = []
        predicted_spec = []

        clean_sentence = remove_punctuation(sentence)
        mapper = Mapper()
        mapper.fill_specialities_pract_areas_dict(features_file_name)

        for feature in mapper.sort_keys():
            if remove_punctuation(feature) in clean_sentence:
                practice_area, specialty = mapper.get_area_and_speciality(feature)
                if practice_area: predicted_areas.append(practice_area.strip())
                if specialty: predicted_spec.append(specialty.strip())
                clean_sentence = clean_sentence.replace(remove_punctuation(feature), ' ')
        return (predicted_areas, predicted_spec)


if __name__ == '__main__':
    pass
