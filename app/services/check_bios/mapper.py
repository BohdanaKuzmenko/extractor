import csv


class Mapper(object):
    def __init__(self):
        self.practice_areas_dict = {}

    def fill_specialities_pract_areas_dict(self, file_name):
        with open(file_name, 'r') as csvfile:
            csv_reader = csv.reader(csvfile, delimiter='\t')
            for row in csv_reader:
                self.practice_areas_dict[row[0].lower()] = [row[1].lower(), row[2].lower()]

    def sort_keys(self):
        keys = self.practice_areas_dict.keys()
        return sorted(keys, key=len, reverse=True)

    def get_area_and_speciality(self, key):
        return self.practice_areas_dict[key]

