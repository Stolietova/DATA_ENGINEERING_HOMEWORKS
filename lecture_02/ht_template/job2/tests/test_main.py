import unittest
from unittest import TestCase, mock

# NB: avoid relative imports when you will write your code
from .. import main


class MainFunctionTestCase(TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        main.app.testing = True
        cls.client = main.app.test_client()


    @mock.patch('lecture_02.ht_template.job2.main.save_sales_to_local_disk')
    def test_return_400_stg_dir_param_missed(
            self,
            get_sales_mock: mock.MagicMock
        ):
        """
        Raise 400 HTTP code when no 'stg_dir' param
        """
        resp = self.client.post(
            '/',
            json={
                'raw_dir': '/tmp/raw_test',
                # no 'stg_dir' set!
            },
        )

        self.assertEqual(400, resp.status_code)

    def test_return_400_raw_dir_param_missed(self):
        """
        Raise 400 HTTP code when no 'raw_dir' param
        """
        resp = self.client.post(
            '/',
            json={
                'stg_dir': '/tmp/stg_test',
                # no 'raw_dir' set!
            },
        )

        self.assertEqual(400, resp.status_code)

    @mock.patch('lecture_02.ht_template.job2.main.save_sales_to_local_disk')
    def test_save_sales_to_local_disk(
            self,
            save_sales_to_local_disk_mock: mock.MagicMock
    ):
        """
        Test whether api.get_sales is called with proper params
        """
        fake_raw_dir = '/tmp/raw_test'
        fake_stg_dir = '/tmp/stg_test'
        self.client.post(
            '/',
            json={
                'raw_dir': fake_raw_dir,
                'stg_dir': fake_stg_dir,
            },
        )

        save_sales_to_local_disk_mock.assert_called_with(
            raw_dir=fake_raw_dir,
            stg_dir=fake_stg_dir,
        )

    @mock.patch('lecture_02.ht_template.job2.main.save_sales_to_local_disk')
    def test_return_201_when_all_is_ok(
            self,
            get_sales_mock: mock.MagicMock
    ):
        fake_raw_dir = '/tmp/raw_test'
        fake_stg_dir = '/tmp/stg_test'

        resp = self.client.post(
        '/',
        json={
            'raw_dir': fake_raw_dir,
            'stg_dir': fake_stg_dir,
        },
        )

        self.assertEqual(201, resp.status_code)
        get_sales_mock.assert_called_once_with(raw_dir=fake_raw_dir, stg_dir=fake_stg_dir)


if __name__ == '__main__':
    unittest.main()