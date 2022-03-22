import pandas as pd
import numpy as np
import matplotlib.pyplot as plt 
#visualisation
# get_ipython().run_line_magic('matplotlib', 'inline')
import missingno as mn


#Read CSV
store_sales_cols = [0, 1, 2, 7, 10, 20, 21]
store_sales_header_list = ["Sold Date", "Sold Time", "Item","Store","Quantity","Net Paid","Inc Tax"]
df_store_sales = pd.read_csv("/dcs/cs346/tpcds/40G/store_sales.dat",
                             sep = '|',
                             names = store_sales_header_list,
                             usecols = store_sales_cols)
print(df_store_sales.head(5))


# print('Shape of the dataset:')
# df_store_sales.shape


# print('Data types of the dataset: ')
# df_store_sales.info()



# print('Describle the dataset')
# df_store_sales.describe()

# print('Cardinality of the columns')
# for col in df_store_sales:
#     cardinality = len(pd.Index(df_store_sales[col]).value_counts())
#     print(df_store_sales[col].name + ": " + str(cardinality))


print('Counts of each store')
store_counts = df_store_sales['Store'].value_counts()
print(store_counts)

print('Visualize Missing value')
fig = mn.matrix(df_store_sales.head(500))
fig_copy = fig.get_figure()
fig_copy.savefig('plots/matrix_plot.png', bbox_inches = 'tight')


df_store_netpaid = df_store_sales.loc[:,['Store','Net Paid']]

df_final = df_store_netpaid.groupby('Store',as_index = False).sum()
df_final.head()


# df_final.plot.bar(x='Store', y="Net Paid", rot=70, title="Net Paid of Stores")
# plt.show(block=True)
# plt.savefig('plots/bar_chart.png', bbox_inches='tight')