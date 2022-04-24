# Copyright 2021 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Classes for converting the Hoppity single AST diff dataset to a PLUR dataset."""
import glob
import json
import os
import tarfile

from absl import logging
import apache_beam as beam
from plur.stage_1.plur_dataset import Configuration
from plur.stage_1.plur_dataset import PlurDataset
from plur.utils import constants
from plur.utils import util
from plur.utils.graph_to_output_example import GraphToOutputExample
from plur.utils.graph_to_output_example import GraphToOutputExampleNotValidError
import tqdm
import networkx as nx
from networkx.drawing.nx_pydot import read_dot

class Code2GraphDataset(PlurDataset):
  def __init__(self,
               stage_1_dir,
               configuration: Configuration = Configuration(),
               transformation_funcs=(),
               filter_funcs=(),
               user_defined_split_range=(),
               num_shards=1000,
               seed=0,
               deduplicate=False):
    self.cooked_one_diff_extracted_dir = None
    self.hoppity_cg_extracted_dir = None
    super().__init__(
        self._DATASET_NAME,
        self._URLS,
        self._GIT_URL,
        self._DATASET_DESCRIPTION,
        stage_1_dir,
        transformation_funcs=transformation_funcs,
        filter_funcs=filter_funcs,
        user_defined_split_range=user_defined_split_range,
        num_shards=num_shards,
        seed=seed,
        configuration=configuration,
        deduplicate=deduplicate)

  def download_dataset(self):
    """Download and extract the tar.gz files."""
    super().download_dataset_using_requests()

  def get_all_raw_data_paths(self):
    """Get paths to all raw data."""
    return None

  def raw_data_paths_to_raw_data_do_fn(self):
    """Returns a beam.DoFn subclass that reads the raw data."""
    return None

def parse_dot_file(file_name):
    try:
        rawGraph: Graph = nx.Graph(read_dot(file_name))
        graph = to_pydot(rawGraph)

        graph_to_output_example = GraphToOutputExample()

        name_to_node_index = {}
        index = 0
        for node in graph.get_node_list():
            #graph_to_output_example.add_node(node[0], node[1], node[2])
            graph_to_output_example.add_node(node.get_label(), node.get_label(), node.get_label())

            name_to_node_index[node.get_name()] = index
            index = index + 1

        edge_index = 0
        for edge in graph.get_edge_list():
            graph_to_output_example.add_edge(edge[0], edge[1], str(edge[2]))
            graph_to_output_example.add_edge(name_to_node_index[edge.get_source()], name_to_node_index[edge.get_destination()], "some_label")
            edge_index = edge_index + 1

        print("processed file:{}".format(file_name))
        return graph_to_output_example
    except Exception as e:
        print("file: {}, {}".format(file_name, e))


  def raw_data_to_graph_to_output_example(self, raw_data):
    """Convert raw data to the unified GraphToOutputExample data structure.

    The hoppity single AST diff task is already represented as a graph.
    We used their input graph structure. For the output, we have 1 to 1 mapping
    to our output format. We try to mirror the implementation details from
    the hoppity data processing stage.

    Args:
      raw_data: A hoppity single AST diff task from the dataset. It is a
        dictionary with keys 'split', 'buggy_graph' and 'edit_operation'. Values
        are the split(train/valid/test) the data belongs, the buggy_graph read
        from the json file and 'edit_operation' read from the text file.

    Raises:
      GraphToOutputExampleNotValidError if the GraphToOutputExample is not
      valid.

    Returns:
      A dictionary with keys 'split' and 'GraphToOutputExample'. Values are the
      split(train/validation/test) the data belongs, and the
      GraphToOutputExample instance.
    """
    split = raw_data['split']

    # TODO: first day, second day: just add one sample and run end to end
    # TODO: resf of the week: add 5 samples and then run end to end
    graph_to_output_example = GraphToOutputExample()

    # add nodes
    graph_to_output_example.add_node(node[0], node[1], node[2])

    # add edges
    graph_to_output_example.add_edge(edge[0], edge[1], str(edge[2]))

    # output
    graph_to_output_example.add_token_output("return sum;")
    return {'split': split, 'GraphToOutputExample': graph_to_output_example}

