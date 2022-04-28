from plur.plur_data_generation import create_dataset

dataset_name = 'manysstubs4j_dataset'
dataset_stage_1_directory = '/tmp/manysstubs4j_dataset/stage_1'
stage_1_kwargs = dict()
dataset_stage_2_directory = '/tmp/manysstubs4j_dataset/stage_2'
stage_2_kwargs = dict()
create_dataset(dataset_name, dataset_stage_1_directory, dataset_stage_2_directory, stage_1_kwargs, stage_2_kwargs)



# dataset_name = 'code2graph_dataset'
# dataset_stage_1_directory = '/tmp/code2graph_dataset/stage_1'
# stage_1_kwargs = dict()
# dataset_stage_2_directory = '/tmp/code2graph_dataset/stage_2'
# stage_2_kwargs = dict()
# create_dataset(dataset_name, dataset_stage_1_directory, dataset_stage_2_directory, stage_1_kwargs, stage_2_kwargs)