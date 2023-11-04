# coding=utf-8
# Copyright 2018-2023 EvaDB
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


import os
import requests
import pandas as pd
import re
from retry import retry
import time
from evadb.catalog.catalog_type import NdArrayType
from evadb.configuration.configuration_manager import ConfigurationManager
from evadb.functions.abstract.abstract_function import AbstractFunction
from evadb.functions.decorators.decorators import forward, setup
from evadb.functions.decorators.io_descriptors.data_types import PandasDataframe


class TinyLLama(AbstractFunction):
    @property
    def name(self) -> str:
        return "TinyLlama"

    @setup(cacheable=True, function_type="text-generation", batchable=True)
    def setup(
        self,
        model="TinyLlama",
        temperature: float = 0,
    ) -> None:
        self.model = model
        self.temperature = temperature

    @forward(
        input_signatures=[
            PandasDataframe(
                columns=["query"],
                column_types=[
                    NdArrayType.STR,
                ],
                column_shapes=[(1,)],
            )
        ],
        output_signatures=[
            PandasDataframe(
                columns=["response"],
                column_types=[
                    NdArrayType.STR,
                ],
                column_shapes=[(1,)],
            )
        ],
    )
    def forward(self, text_df):
        results = []
        
        for ind in text_df.index:
            converted_content =  str(text_df['input'][ind])
            string_without_digits = re.sub(r'\d', '', converted_content)
            cleaned_string = ' '.join(string_without_digits.split())
            time.sleep(1.9)
            API_URL = "https://api-inference.huggingface.co/models/PY007/TinyLlama-1.1B-Chat-v0.3"
            headers = {"Authorization": "Bearer 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"}
            response = {
                "inputs": cleaned_string,
            }
            result = requests.post(API_URL, headers=headers, json=response)
            convert_result = result.content.decode("utf-8")
            index = convert_result.find("generated_text")
            convert_result = convert_result[index + len("generated_text")+3:]
            results.append(convert_result)
        df = pd.DataFrame({"response": results})
        return df
