import os
import json
import tempfile
import unittest
from unittest import TestCase, mock

from lecture_02.ht_template.job1.dal.local_disk import save_to_disk


class SaveToDiskTestCase(TestCase):
    def setUp(self) -> None:
        self.tmpdir_obj = tempfile.TemporaryDirectory()
        self.tmpdir = self.tmpdir_obj.name
        
    def tearDown(self) -> None:
        self.tmpdir_obj.cleanup()

    def test_save_to_disk_writes_json(self):
        self.data = [
            {"client": "Alice", "product": "Phone", "price": 1000},
            {"client": "Bob", "product": "TV", "price": 1500}
        ]
        
        date = "2022-08-09"
        file_name = f"sales_{date}.json"
        os.makedirs(self.tmpdir, exist_ok=True)
        file_path = os.path.join(self.tmpdir, file_name)

        save_to_disk(self.data, file_path)

        self.assertTrue(os.path.exists(file_path), f"Expected file {file_name} to exist")

        with open(file_path, "r", encoding="utf-8") as f:
            loaded = json.load(f) 

        self.assertIsInstance(loaded, list)  
        self.assertEqual(loaded, self.data)       


if __name__ == '__main__':
    unittest.main()