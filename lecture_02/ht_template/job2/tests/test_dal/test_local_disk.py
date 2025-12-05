import os
import json
import tempfile
from fastavro import reader
from unittest import TestCase, mock

from lecture_02.ht_template.job2.dal import local_disk


class SaveToDiskTestCase(TestCase):
    def setUp(self) -> None:
        self.tmpdir_obj = tempfile.TemporaryDirectory()
        self.tmpdir = self.tmpdir_obj.name

        self.data = [
            {"client": "Alice", "product": "Phone", "price": 1000},
            {"client": "Bob", "product": "TV", "price": 1500}
        ]

        date = '2022-08-09'
        file_name_json = f'sales_{date}.json'
        file_name_avro = f'sales_{date}.avro'
        self.json_file_path = os.path.join(self.tmpdir, file_name_json)
        self.avro_file_path = os.path.join(self.tmpdir, file_name_avro)

        with open(self.json_file_path, 'w', encoding='utf-8') as f:
            json.dump(self.data, f)

    def tearDown(self) -> None:
        self.tmpdir_obj.cleanup()

    def test_convert_json_to_avro_creates_file(self):
        local_disk.convert_json_to_avro(str(self.json_file_path), str(self.avro_file_path))

        self.assertTrue(os.path.exists(self.avro_file_path))

        with open(self.avro_file_path, 'rb') as f:
            avro_reader = reader(f)
            records = list(avro_reader)
        self.assertEqual(records, self.data)

if __name__ == '__main__':
    import unittest
    unittest.main()