"""
This file contains the controller that accepts command via HTTP
and trigger business logic layer
"""
import os
from flask import Flask, request
from flask import typing as flask_typing

from lecture_02.ht_template.job2.bll.sales_api import save_sales_to_local_disk




app = Flask(__name__)


@app.route('/', methods=['POST'])
def main() -> flask_typing.ResponseReturnValue:
    """
    Controller that accepts command via HTTP and
    trigger business logic layer

    """
    input_data: dict = request.json
  
    raw_dir: str = input_data.get('raw_dir')
    stg_dir: str = input_data.get('stg_dir')

    if not raw_dir or not stg_dir:
        return {
            "message": "raw_dir parameter missed",
        }, 400

    save_sales_to_local_disk(raw_dir=raw_dir, stg_dir=stg_dir)

    return {
               "message": "Data retrieved successfully from API",
           }, 201


if __name__ == "__main__":
    app.run(debug=True, host="localhost", port=8082)
