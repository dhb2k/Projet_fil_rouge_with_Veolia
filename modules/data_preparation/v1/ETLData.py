#!/usr/bin/env python

# imports
import pandas as pd

class ETLData:
    '''
        The goal of this class is to be able to easily load and transform dataset using method chaining.
        Allows for powerful computations to be carried out on dataset while being very compact in terms of code.
        
        PARAMETERS
        energy_data_path: [str] Path to energy data, will be loaded as dataframe in data attribute.
        temperature_data_path: [str] (default:None): TODO
        
        EXAMPLE
        dataset = ETLData('./data/dataset1.csv')

        efficiency_fn = (lambda e_in, e_out: e_out/e_in)
        positive = (lambda x: x>0)

        (
        dataset.to_numeric(['energy_input_in_mwh', 'energy_output_in_mwh'])
        .filter_by_column_value('energy_input_in_mwh', positive)
        .filter_by_column_value('energy_output_in_mwh', positive)
        .to_timeseries('timestamp_local')
        .compute_column(column_name='efficiency', fn_arg_col_names=['energy_input_in_mwh', 'energy_output_in_mwh'], fn_compute=efficiency_fn)
        )
    '''
# TODO: __init__: integrate temperature data
# TODO: method: resample timeseries data

    def __init__(self, energy_data_path, temperature_data_path=None):
        
        # load energy data
        self.data = pd.read_csv(energy_data_path)
        
            
    def get_data(self):
        return self.data
    
    def to_numeric(self, columns):
        '''
        Format columns to numeric
        
        PARAMETERS
        columns: [list] List of column names to be formatted
        
        EXAMPLE
        dataset1 = ETLData('./data/dataset1.csv')
        dataset1.to_numeric(['energy_input_in_mwh', 'energy_output_in_mwh'])
        '''
        for col in columns:
            self.data[col] = self.data[col].str.replace(',','.')
            self.data[col] = pd.to_numeric(self.data[col])

        return self
    
    def to_timeseries(self, timestamp_column):
        '''
        Format following columns to numeric
        
        PARAMETERS
        timestamp_column: [str] Name of column to use as timeseries data
        
        EXAMPLE
        dataset1 = ETLData('./data/dataset1.csv')
        dataset1.to_timeseries('timestamp_local')
        '''
        self.data[timestamp_column] = pd.to_datetime(self.data[timestamp_column])
        self.data.sort_values(by=timestamp_column, inplace = True) 
        self.data.set_index([timestamp_column], inplace=True)
        
        return self
    
    def filter_by_column_value(self, column_name, filter_function, save=True):
        '''
        Filter column by applying filter_function to values in column_name
        
        PARAMETERS
        save : [boolean] (default:True): 
            - if True object data will be updated with filtered version of dataframe, updated object will be returned
            - if False, object data will not be updated, filtered version of dataframe will be returned
        column_name : [str] name of column to be filtered
        filter_function : [fnc] function to be applied to values in column being filtered
        
        EXAMPLE
        dataset1 = ETLData('./data/dataset1.csv')
        dataset1.filter_by_column_value(column_name='energy_input_in_mwh', 
                                       filter_function = lambda x: x>0)
        '''
        if save:
            self.data = self.data[self.data[column_name].apply(filter_function)]
            return self
        else:
            return self.data[self.data[column_name].apply(filter_function)]

        
    def compute_column(self, column_name, fn_arg_col_names, fn_compute, save=True):
        '''
        Assign new or existing column (indicated by column_name), values computed using the fn_compute function.
        The function uses data dataframe columns (indicated by fn_arg_col_names) as arguments.
        
        PARAMETERS
        column_name: [str] Name of new or existing column to be assigned computed values.
        fn_arg_col_names: [list] List of column names to be used as argument to fn_compute. 
                                 Order must match fn_compute arguments.
        fn_compute: [fnc] Defines computation to be carried out
        
        EXAMPLE
        dataset1 = ETLData('./data/dataset1.csv')
        efficiency_fn = (lambda e_in, e_out: e_out/e_in)
        (
        dataset.to_numeric(['energy_input_in_mwh', 'energy_output_in_mwh'])
               .compute_column(column_name='efficiency', fn_arg_col_names=['energy_input_in_mwh', 'energy_output_in_mwh'], fn_compute=efficiency_fn)
        )
        '''
            
        def generate_arg_str(col):
            return 'self.data[' + "'" + col + "'" + ']'
        
        def generate_args_str(column_names):
            args = ''
            for i in range(len(column_names)-1):
                args = args + generate_arg_str(column_names[i]) + ','
            args = args + generate_arg_str(column_names[len(column_names)-1])
            return args
        
        def generate_fn_str(function_name, column_names):
            return function_name + '(' + generate_args_str(column_names)+ ')'
        
        if save:
            self.data[column_name] = eval(generate_fn_str('fn_compute', fn_arg_col_names))
            return self
        else:
            return eval(generate_fn_str('fn_compute', fn_arg_col_names))
        