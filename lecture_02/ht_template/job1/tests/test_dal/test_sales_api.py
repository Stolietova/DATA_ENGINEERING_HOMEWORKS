import unittest 
from unittest import TestCase, mock

from lecture_02.ht_template.job1.dal import sales_api


class GetSalesTestCase(TestCase):
   
    @mock.patch("lecture_02.ht_template.job1.dal.sales_api.requests.get")

    def test_get_sales_api(self, mock_get):
        
        mock_get.return_value.raise_for_status.return_value = None
        mock_get.return_value.json.side_effect = [
            [{"client": "Alice", "product": "Phone", "price": 1000}],  
            [{"client": "Bob", "product": "TV", "price": 1500}],       
            []]
        
        result = sales_api.get_sales("2022-08-09")

        self.assertIsInstance(result, list)
        self.assertEqual(result, [
            {"client": "Alice", "product": "Phone", "price": 1000},
            {"client": "Bob", "product": "TV", "price": 1500}])
     

if __name__ == '__main__':
    unittest.main()