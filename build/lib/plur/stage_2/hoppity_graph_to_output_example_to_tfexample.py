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

"""Class for converting Hoppity PLUR dataset to TFExamples.
"""
import functools

from plur.stage_2.graph_to_output_example_to_tfexample import GraphToOutputExampleToTfexample
from plur.utils import constants


class HoppityGraphToOutputExampleToTfexample(GraphToOutputExampleToTfexample):
  """Main class for converting GraphToOutputExample to TfExample.

  Hoppity dataset requires a special stage 2 data process. In the hoppity
  output, when the prediction for a node's value is oov, the model is given
  credit for predicting a special 'UNKNOWN' token. Therefore we add this
  special token in the output vocabulary, and replace all output node values
  with 'UNKNOWN', if it is not part of the output vobucalary, or it does
  not exists among all graph node values (cannot be copied from the graph).
  For example, 'replace_value POINTER(1) VeryRareName' with become
  'replace_value POINTER(1) UNKNOWN', and it will the ground truth target.
  In this way, we can still use exact sequence match to measure all
  transformation accuracies, and the overall accuracy.
  """

  def build_and_save_vocab(self, p):
    """Build and save vocab files.

    We compute vocabulary for the node types, node labels, edge types and
    output tokens. The difference to GraphToOutputExampleToTfexample.
    build_and_save_vocab() is that we always add
    constants.HOPPITY_OUTPUT_UNKNOWN_TOKEN to the output token vocabulary.

    Args:
      p: The root of beam pipeline.
    """
    output_token_reserved_tokens = constants.RESERVED_TOKENS + [
        constants.HOPPITY_OUTPUT_UNKNOWN_TOKEN]
    super().build_and_save_vocab(
        p, output_token_reserved_tokens=output_token_reserved_tokens)

  def convert_and_write_tfexample(self, p):
    """Pipeline that converts Hoppity GraphToOutputExample to TFExample.

    The difference compared to GraphToOutputExampleToTfexample.
    convert_and_write_tfexample() is that we always add transform_oov_to_unknown
    function to the tuple of transformation functions for all splits.

    Args:
      p: The beam pipeline root.
    """
    _, _, _, output_token_vocab_dict = self.read_vocab_files()
    transform_oov_to_unknown_fn = functools.partial(
        transform_oov_to_unknown,
        output_token_vocab_dict=output_token_vocab_dict)
    super().convert_and_write_tfexample(
        p, additional_train_transformation_funcs=(transform_oov_to_unknown_fn,),
        additional_valid_transformation_funcs=(transform_oov_to_unknown_fn,),
        additional_test_transformation_funcs=(transform_oov_to_unknown_fn,))


def transform_oov_to_unknown(graph_to_output_examples, output_token_vocab_dict):
  """Transform oov node value to a special 'UNKNOWN' keyword.

  The preprocessing of Hoppity transforms a oov node value, ie. a node that can
  not be generated by the output vocabulary, or by copy from the input node,
  to a special 'UNKNOWN' keyword. This can only be done at stage 2 when
  we create the output vocabulary. Therefore this function should be one of
  the transformations that are applied on GraphToOutputExample when loading
  them from disk.

  Args:
    graph_to_output_examples: A hoppity GraphToOutputExample instance.
    output_token_vocab_dict: A dictionary mapping the output vocabulary to
      integers.

  Returns:
    A Hoppity GraphToOutputExample instance, where a oov node value is replaced
    with 'UNKNOWN'.
  """
  output = graph_to_output_examples.get_output()
  # Only add_node and replace_value operations have node value as output.
  if output[0]['token'] not in [constants.HOPPITY_ADD_NODE_OP_NAME,
                                constants.HOPPITY_REPLACE_VAL_OP_NAME]:
    return graph_to_output_examples

  # All possible node values are the output vocabulary and all input node
  # values.
  node_labels = graph_to_output_examples.get_node_labels()
  output_token_vocabulary = list(output_token_vocab_dict.keys())
  all_possible_vocab = node_labels + output_token_vocabulary
  # The node value is always the last output. We check that if it is in all
  # possible node values. If not, replace it with 'UNKNOWN'
  if output[-1]['token'] not in all_possible_vocab:
    output[-1]['token'] = constants.HOPPITY_OUTPUT_UNKNOWN_TOKEN

  return graph_to_output_examples

