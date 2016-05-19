from unittest import TestCase
from app.services.check_bios.main import Extractor
from pandas import DataFrame


class TestMain(TestCase):
    def test_get_ai_results(self):
        urls = ["url1", "url2", "url3", "url4", "url5", "url6", "url7", "url8", "url9", "url10"]
        text = ["test text1", "test text2", "test text3", "text4", "test text5", "test text6", "test text7",
                "test text8", "test text9", "test text10"]
        data = [list(i) for i in zip(urls, text)]
        test = DataFrame(data, columns=['profileUrl', 'attorneyBio'])

        content_regexes_df = DataFrame([["CN1", "test"]], columns=['Content REG ID', 'KeyWord'])

        joined_regex_df = DataFrame([["test text", 'JX1', 90, 'Litigation', "Bank", "CN1", 'CX1']],
                                    columns=["regex", 'regex_id', 'score', 'pract_areas',
                                             'specialties', 'content_regex', 'context_regex'])
        test_extractor = Extractor(joined_regex_df, content_regexes_df)
        result_df = test_extractor.get_ai_results(test)
        print(result_df)

        self.assertEqual(result_df.profileUrl.count(), 9)
        self.assertTrue(result_df[result_df.profileUrl.str.contains("url4")].empty)
