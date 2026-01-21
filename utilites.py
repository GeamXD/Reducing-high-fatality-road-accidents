
def get_labels_easy(dataframe, field_name):
    """
            Utility function to extract labels from a dataframe
    :param dataframe:
    :param field_name:
    :return:
    """
    labels = dataframe[dataframe['field name'] == field_name].iloc[:, 2:4]
    labels = dict(zip(labels.iloc[:, 0].astype(int), labels.iloc[:, 1]))
    return labels