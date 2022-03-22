import matplotlib.pyplot as plt 
import numpy as np
import pandas as pd
import dask.dataframe as dd
import dask.array as da
import dask.bag as db
import missingno as mn


#Read CSV
store_sales_cols = [0, 1, 2, 7, 10, 20, 21]
store_sales_header_list = ["Sold Date", "Sold Time", "Item","Store","Quantity","Net Paid","Inc Tax"]
df_store_sales = dd.read_csv("/dcs/cs346/tpcds/40G/store_sales.dat",
                             sep = '|',
                             names = store_sales_header_list,
                             usecols = store_sales_cols)


store_cols = [0, 7]
store_header_list = ["Store","Floor Space"]
df_store = dd.read_csv("/dcs/cs346/tpcds/40G/store.dat",
                        sep = '|',
                        names = store_header_list,
                        usecols = store_cols)

# print('------------------------')
# print('Shape of the dataset:')
# df_store_sales.shape

# print('------------------------')
# print('Data types of the dataset: ')
# df_store_sales.info()



# print('Describle the dataset')
# df_store_sales.describe().to_csv("store_sales_describe.csv", index=True)
# df_store.describe().to_csv("store_describe.csv", index=True)




# print('Visualize Missing value')
# fig = mn.matrix(df_store.head(500))
# fig_copy = fig.get_figure()
# fig_copy.savefig('plots/matrix_plot_store.png', bbox_inches = 'tight')


plt.figure(figsize=(20,10))
plt.hist(df_store_sales['Sold Date'], bins=175 ,edgecolor='black', density=1)
plt.show() 
plt.savefig('plots/histogram.png', bbox_inches='tight')