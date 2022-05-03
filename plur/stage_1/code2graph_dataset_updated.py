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

"""Classes for converting the code2graph dataset to a PLUR dataset."""
import string

import apache_beam as beam
from networkx import Graph

from plur.stage_1.plur_dataset import Configuration
from plur.stage_1.plur_dataset import PlurDataset
from plur.utils.graph_to_output_example import GraphToOutputExample
from plur.utils.graph_to_output_example import GraphToOutputExampleNotValidError

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
from networkx.drawing.nx_pydot import to_pydot

class Code2graphDatasetUpdated(PlurDataset):
  """DummyDataset that contains random data, it is used for testing PlurDataset."""
  _URLS = {}
  _GIT_URL = {}
  _DATASET_NAME = 'dummy_dataset'
  _DATASET_DESCRIPTION = """\
  This dataset is only used for test the data generation process, all data are
  generated randomly.
  """

  def __init__(self,
               stage_1_dir,
               configuration: Configuration = Configuration(),
               transformation_funcs=(),
               filter_funcs=(),
               user_defined_split_range=(),
               num_shards=1000,
               seed=0,
               num_random_graph=1000,
               min_node_per_graph=100,
               max_node_per_graph=1000,
               deduplicate=False):
    self.code2graph_dir = None
    # self.num_random_graph = num_random_graph
    # self.min_node_per_graph = min_node_per_graph
    # self.max_node_per_graph = max_node_per_graph
    super().__init__(self._DATASET_NAME, self._URLS, self._GIT_URL,
                     self._DATASET_DESCRIPTION, stage_1_dir,
                     transformation_funcs=transformation_funcs,
                     filter_funcs=filter_funcs,
                     user_defined_split_range=user_defined_split_range,
                     num_shards=num_shards, seed=seed,
                     configuration=configuration, deduplicate=deduplicate)

  def download_dataset(self):
    """All data are generated on the fly, so we 'pass' here."""
    #super().download_dataset_using_requests()
    # self.cooked_one_diff_extracted_dir = os.path.join(
    #     self.raw_data_dir, 'cooked-full-fmt-shift_node')
    # self.hoppity_cg_extracted_dir = os.path.join(self.raw_data_dir,
    #                                              'hoppity_cg')
    # tarfiles_to_extract = []
    # tarfiles_to_extract = util.check_need_to_extract(
    #     tarfiles_to_extract, self.cooked_one_diff_extracted_dir,
    #     'cooked-one-diff.gz')
    #
    # tarfiles_to_extract = util.check_need_to_extract(
    #     tarfiles_to_extract, self.hoppity_cg_extracted_dir, 'hoppity_cg.tar.gz')
    #
    # for filename in tarfiles_to_extract:
    #     dest = os.path.join(self.raw_data_dir, filename)
    #     with tarfile.open(dest, 'r:gz') as tf:
    #         for member in tqdm.tqdm(
    #                 tf.getmembers(), unit='file',
    #                 desc='Extracting {}'.format(filename)):
    #             tf.extract(member, self.raw_data_dir)
    self.code2graph_dir = os.path.join(
        self.raw_data_dir, 'dotFiles')


  def get_all_raw_data_paths(self):
    """All data are generated on the fly, only return a dummy value."""
    return glob.glob(
        os.path.join(self.code2graph_dir, '*.dot'))

  def raw_data_paths_to_raw_data_do_fn(self):
    """Use RandomDataGenerator to generate random graphs."""
    # return RandomDataGenerator(self.num_random_graph,
    #                            super().get_random_split,
    #                            self._generate_random_graph_to_output_example)
    with open(os.path.join(self.code2graph_dir,
                           'code2Graph.dot')) as f:
        train_data_filenames = set(f.read())
    with open(os.path.join(self.code2graph_dir, 'code2Graph.dot')) as f:
        validation_data_filenames = set(f.read())
    with open(os.path.join(self.code2graph_dir,
                           'code2Graph.dot')) as f:
        testing_data_filenames = set(f.read())
    return DotParser(super().get_random_split,
                     bool(self.user_defined_split_range),
                     train_data_filenames, validation_data_filenames,
                     testing_data_filenames,
                     self.code2graph_dir)


  def _generate_random_graph_to_output_example(self):
    """Generate a random GraphToOutputExample.

    The input graph is a chain where the node type are uppercase ASCII and the
    node label are lowercase ASCII. All nodes are connected to the previous node
    with lower node id to form a chain. The output is a combination of all
    possible output types (token/pointer/class).

    Returns:
      A random GraphToOutputExample.
    """
    graph_to_output_example = GraphToOutputExample()

    for i in range(self.random.randint(self.min_node_per_graph,
                                       self.max_node_per_graph)):
      node_type = self.random.choice(string.ascii_uppercase)
      node_label = self.random.choice(string.ascii_lowercase)
      graph_to_output_example.add_node(
          i, node_type, node_label, is_repair_candidate=(i % 2 == 0))

    for i in range(len(graph_to_output_example.get_nodes()) - 1):
      graph_to_output_example.add_edge(i, i+1, 'NEXT_NODE')

    graph_to_output_example.add_token_output('foo')
    graph_to_output_example.add_pointer_output(
        self.random.choice(
            list(range(len(graph_to_output_example.get_nodes())))))
    graph_to_output_example.add_class_output('0')

    return graph_to_output_example

  def raw_data_to_graph_to_output_example(self, raw_data):
    """Return raw data as it is."""
    # split = raw_data['split']
    # The raw data is already a GraphToOutputExample instance.
    # graph_to_output_example = raw_data['data']
    #
    # for transformation_fn in self.transformation_funcs:
    #   graph_to_output_example = transformation_fn(graph_to_output_example)
    #
    # if not graph_to_output_example.check_if_valid():
    #   raise GraphToOutputExampleNotValidError(
    #       'Invalid GraphToOutputExample found.')
    #
    # for filter_fn in self.filter_funcs:
    #   if not filter_fn(graph_to_output_example):
    #     graph_to_output_example = None
    #     break
    #
    # return {'split': split, 'GraphToOutputExample': graph_to_output_example}

    code2graph = raw_data ['code2graph']
    graph_to_output_example = GraphToOutputExample()

    name_to_node_index = {}
    node_index = 0
    for node in code2graph.get_node_list():
        graph_to_output_example.add_node(node.obj_dict['sequence'], node.obj_dict['type'], node.obj_dict['name'])
        name_to_node_index[node.get_name()] = node_index
        node_index = node_index + 1

    edge_index = 0
    # Each edge is [src node id, dst node id, edge type]
    for edge in code2graph.get_edge_list():
        # There are duplicate edges in the raw dataset, therefore we ignore them
        # here.
        try:
            graph_to_output_example.add_edge(edge.obj_dict['points'][0], edge.obj_dict['points'][1], str(edge.obj_dict['type']))
            graph_to_output_example.add_edge(name_to_node_index[edge.get_source()], name_to_node_index[edge.get_destination()], "some_label")
            edge_index = edge_index + 1
        except ValueError:
            logging.warning('Duplicate edge in %s, ignoring it for now.',
                            self.dataset_name)

    for transformation_fn in self.transformation_funcs:
        graph_to_output_example = transformation_fn(graph_to_output_example)

    if not graph_to_output_example.check_if_valid():
        raise GraphToOutputExampleNotValidError(
            'Invalid GraphToOutputExample found {}'.format(
                graph_to_output_example))

    for filter_fn in self.filter_funcs:
        if not filter_fn(graph_to_output_example):
            graph_to_output_example = None
            break

    return {'GraphToOutputExample': graph_to_output_example}


class DotParser(beam.DoFn):
  """Class that generates a random GraphToOutputExample."""

  # def __init__(self, num_random_graph, random_split_fn,
  #              generate_random_graph_to_output_example_fn):
  #   self.num_random_graph = num_random_graph
  #   self.random_split_fn = random_split_fn
  #   self._generate_random_graph_to_output_example_fn = (
  #       generate_random_graph_to_output_example_fn)

  def __init__(self, random_split_fn, use_random_split, train_data_filenames,
               validation_data_filenames, testing_data_filenames,
               code2graph_dir):
      self.random_split_fn = random_split_fn
      self.use_random_split = use_random_split
      self.train_data_filenames = train_data_filenames
      self.validation_data_filenames = validation_data_filenames
      self.testing_data_filenames = testing_data_filenames
      self.code2 = code2graph_dir

  # def process(self, _):
  #   for _ in range(self.num_random_graph):
  #     yield {'split': self.random_split_fn(),
  #            'data': self._generate_random_graph_to_output_example_fn()}
  def _read_data(self, file_path):
      """Read the buggy graph and edit operation JSON files."""
      file_name=file_path
      # filename_prefix = file_path
      # edit_operation_file_path = os.path.join(self.cooked_one_diff_extracted_dir,
      #                                         (filename_prefix + '_gedit.txt'))
      with open(file_path) as f:
          rawGraph: Graph = nx.Graph(read_dot(file_name))
          graph = to_pydot(rawGraph)
      #     buggy_graph = json.load(f)
      # with open(edit_operation_file_path) as f:
      #     edit_operation = json.load(f)[0]['edit']
      # return buggy_graph, edit_operation
      return graph

  # def _get_split(self, file_path):
  #     """Get the Hoppity predefined split with the filename prefix."""
  #     filename_prefix = os.path.basename(file_path)[:-11]
  #     if filename_prefix in self.train_data_filenames:
  #         return constants.TRAIN_SPLIT_NAME
  #     elif filename_prefix in self.validation_data_filenames:
  #         return constants.VALIDATION_SPLIT_NAME
  #     elif filename_prefix in self.testing_data_filenames:
  #         return constants.TEST_SPLIT_NAME
  #     else:
  #         return None
  def process(self, file_path):
      """Function to read each json file.
      Args:
        file_path: Path to a raw data file.
      Yields:
        A dictionary with 'split', 'buggy_graph' and 'edit_operation' as keys.
        The value of the 'split' field is the split (train/valid/test) that the
        data belongs to. The value of the 'buggy_graph' is the parsed buggy graph.
        The value of the 'edit_operation' is the edit operation string.
      """

      # split = self._get_split(file_path)
      # if split is None:
      #     return
      graph = self._read_data(file_path)
      yield {
          # 'split': self.random_split_fn() if self.use_random_split else split,
          # 'buggy_graph': buggy_graph,
          # 'edit_operation': edit_operation
          'code2graph': graph
      }

