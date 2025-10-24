"""This file contains the split_data function
which splits the table created after running the final_df function into
train and test set."""

from recommenders.datasets.python_splitters import python_stratified_split
from recommenders.datasets.python_splitters import python_chrono_split
from recommenders.datasets.python_splitters import python_random_split


def split_data(table, **kwargs):
    """A simple function for spliting the final data frame after using the *final_df* function.

    It returns the train and test sets that are going to be used in order to train the SAR model.

    Args:
        table (data frame): table column name after running the *final_df* function
        split (str): the split version that we want to use. Default value 'chrono'
        ratio (float): ratio value. Default value 0.7

    Returns:

    tuple: Splits of the input data frame as pandas.DataFrame.

    """

    split = kwargs.get('split', 'chrono')
    ratio = kwargs.get('ratio', 0.70)
    filter_by = kwargs.get('filter_by', 'user')

    assert split in ['chrono', 'random',
                     'stratified'],  "split must be 'random', 'chrono' or 'stratified'"

    # selecting split type
    if split == 'chrono':
        train, test = python_chrono_split(table,
                                          col_user='userID',
                                          col_item='itemID',
                                          col_timestamp='timestamp',
                                          ratio = ratio,
                                          filter_by = filter_by)

    elif split == 'random':
        train, test = python_random_split(table)

    elif split == 'stratified':
        train, test = python_stratified_split(table,
                                              col_user='userID',
                                              col_item='itemID')

    return train, test
