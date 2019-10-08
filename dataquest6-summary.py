#%% [markdown]
# # Data Cleaning and Analyst
# ## Data Aggregation
# ### Groupby Objects
#
# | Select by Label	| Syntax |
# | --- | --- |
# | Single column| GroupBy["col1"] |
# | List of columns | GroupBy[["col1", "col2"]] |

#%%
# Create a GroupBy Object
import pandas as pd
happiness2015 = pd.read_csv("World_Happiness_2015.csv")
first_5 = happiness2015[:5]
first_5.info()

#%%
mean_happiness = {}
regions = happiness2015['Region'].unique()

for row in regions:
    region_group = happiness2015[happiness2015['Region'] == row]
    region_mean = region_group['Happiness Score'].mean()
    mean_happiness[row] = region_mean
mean_happiness

#%%
happiness2015['Happiness Score'].plot(kind='bar', title='Happiness Scores', ylim=(0,10))

#%%
so_asia = happiness2015[happiness2015['Region'] == 'Southern Asia']
so_asia.plot(x='Country', y='Happiness Score', kind='barh', title='Southern Asia Happiness Scores', xlim=(0,10))

#%%
# df.grouded('col')

happiness2015['Region'].value_counts() 
grouped = happiness2015.groupby('Region')
grouped.groups # return the index of each group

#%%
aus_nz = grouped.get_group('Australia and New Zealand') # return group 'Australia and New Zealand'
aus_nz

#%%
means = grouped.mean() # return the mean by each group
means

#%%
happy_grouped = grouped['Happiness Score'] # get 'Happiness Score' column from each `Region` column
happy_mean = happy_grouped.mean() # return the mean of 'Happiness Score' with 'Region' as the index
happy_mean

#%%
# Testing the groupby to iloc

grouped = happiness2015.groupby('Region')
grouped.get_group('North America')
north_america = happiness2015.iloc[[4,14]] # using manually
na_group = grouped.get_group('North America') # using groupby
equal = north_america == na_group
equal

#%%
# Syntax `groupby.agg([func_name1, func_name2, func_name3])`

import numpy as np
grouped = happiness2015.groupby('Region')
happy_grouped = grouped['Happiness Score']

def dif(group):
    return (group.max() - group.mean())
mean_max_dif = happy_grouped.agg(dif)
mean_max_dif

#%%
happiness2015.groupby('Region')['Happiness Score'].agg(dif)

#%%
happy_mean_max = happy_grouped.agg([np.mean,np.max])
happy_mean_max

#%%
# 'Region` as index & `Happiness Score` as values
happiness_means = happiness2015.groupby('Region')['Happiness Score'].mean()
happiness_means

#%%
pv_happiness = happiness2015.pivot_table(values='Happiness Score', index='Region', aggfunc=np.mean, margins=True)
pv_happiness.plot(kind='barh', title='Mean Happiness Scores by Region', xlim=(0,10), legend=False)

so_asia = happiness2015[happiness2015['Region'] == 'Southern Asia']
so_asia.plot(x='Country', y='Happiness Score', kind='barh', title='Southern Asia Happiness Scores', xlim=(0,10))

world_mean_happiness = happiness2015["Happiness Score"].mean()

#%%
grouped = happiness2015.groupby("Region")[['Happiness Score','Family']]
happy_family_stats = grouped.agg([np.min, np.max, np.mean])
happy_family_stats

#%%
pv_happy_family_stats = happiness2015.pivot_table(['Happiness Score','Family'], 'Region', aggfunc = [np.min, np.max, np.mean], margins = True)

#%% [markdown]
# ### COMMON AGGREGATION METHODS
#
# mean(): Calculates the mean of groups
#
# sum(): Calculates the sum of group values
# 
# size(): Calculates the size of groups
# 
# count(): Calculates the count of values in groups
# 
# min(): Calculates the minimum of group values
# 
# max(): Calculates the maximum of group values

#%% [markdown]
# ## Combining Data With Pandas 

#%%
import pandas as pd
happiness2015 = pd.read_csv("World_Happiness_2015.csv")
happiness2016 = pd.read_csv("World_Happiness_2016.csv")
happiness2017 = pd.read_csv("World_Happiness_2017.csv")

happiness2015["Year"] = 2015
happiness2016["Year"] = 2016
happiness2017["Year"] = 2017

head_2015 = happiness2015[['Country','Happiness Score', 'Year']].head(3)
head_2016 = happiness2016[['Country','Happiness Score', 'Year']].head(3)

concat_axis0 = pd.concat([head_2015,head_2016], axis = 0)
concat_axis1 = pd.concat([head_2015,head_2016], axis = 1)

question1 = concat_axis0.shape[0]
question2 = concat_axis1.shape[0]

#%%
concat_axis0 # combine to down

#%%
concat_axis1 # combine to left

#%%
# Combining two different shape dataframe
head_2015 = happiness2015[['Year','Country','Happiness Score', 'Standard Error']].head(4)
head_2016 = happiness2016[['Country','Happiness Score', 'Year']].head(3)

concat_axis2 = pd.concat([head_2015,head_2016], axis = 0)
rows = concat_axis2.shape[0]
columns = concat_axis2.shape[1]
concat_axis2

#%%
# Clear the exist index
concat_update_index = pd.concat([head_2015,head_2016], axis = 0, ignore_index = True)
concat_update_index

#%%
# Combining two dataframe by column
three_2015 = happiness2015[['Country','Happiness Rank','Year']].iloc[2:5]
three_2016 = happiness2016[['Country','Happiness Rank','Year']].iloc[2:5]

merged = pd.merge(left=three_2015, right=three_2016, on='Country') # inner
merged

#%%
merged_left = pd.merge(left=three_2015, right=three_2016, how='left', on='Country') # outer based on "three_2015"
merged_left

#%%
merged_left_updated = pd.merge(left=three_2016, right=three_2015, how='left', on='Country') # outer based on "three_2016"
merged_left_updated

#%%
merged_suffixes = pd.merge(left=three_2015, right=three_2016, how='left', on='Country', suffixes = ('_2015', '_2016'))
merged_suffixes

#%%
merged_updated_suffixes = pd.merge(left=three_2016, right=three_2015, how = 'left', on='Country', suffixes = ('_2016', '_2015'))
merged_updated_suffixes

#%%
import pandas as pd
four_2015 = happiness2015[['Country','Happiness Rank','Year']].iloc[2:6]
three_2016 = happiness2016[['Country','Happiness Rank','Year']].iloc[2:5]

merge_index = pd.merge(left = four_2015,right = three_2016, left_index = True, right_index = True, suffixes = ('_2015','_2016')) # keep the index from original data
merge_index

#%%
merge_index_left = pd.merge(left = four_2015,right = three_2016, left_index = True, right_index = True, how = "left", suffixes = ('_2015','_2016'))
rows = merge_index_left.shape[0]
columns = merge_index_left.shape[1]
merge_index_left # the result contain only common index

#%%
happiness2017.rename(columns={'Happiness.Score': 'Happiness Score'}, inplace=True)

combined = pd.concat([happiness2015, happiness2016, happiness2017], axis = 0)

pivot_table_combined = combined.pivot_table(index='Year', values='Happiness Score', aggfunc = np.mean)

pivot_table_combined.plot(kind='barh', title='Mean Happiness Scores by Year', xlim=(0,10))

#%% [markdown]
# Inner: only includes elements that appear in both dataframes with a common key
# 
# Outer: includes all data from both dataframes
# 
# Left: includes all of the rows from the "left" dataframe along with any rows from the "right" dataframe with a common key; the result retains all columns from both of the original dataframes
# 
# Right: includes all of the rows from the "right" dataframe along with any rows from the "left" dataframe with a common key; the result retains all columns from both of the original dataframes

#%% [markdown]
# | | pd.concat() | pd.merge() |
# | --- | --- | --- |
# | Default Join Type | Outer | Inner |
# Can Combine More Than Two Dataframes at a Time? |	Yes | No |
# Can Combine Dataframes Vertically (axis=0) or Horizontally (axis=1)? | Both | Horizontally |
# | Syntax | Concat (Vertically)<br>concat([df1,df2,df3])<br><br>Concat (Horizontally)<br>concat([df1,df2,df3], axis = 1) | Merge (Join on Columns)<br>merge(left = df1, right = df2, how = 'join_type', on = 'Col')<br><br>Merge (Join on Index)<br>merge(left = df1, right = df2, how = 'join_type', left_index = True, right_index = True) |

#%% [markdown]
# ### Transforming Data with Pandas

#%%
mapping = {'Economy (GDP per Capita)': 'Economy', 'Health (Life Expectancy)': 'Health', 'Trust (Government Corruption)': 'Trust' }

happiness2015 = happiness2015.rename(mapping, axis =1)

#%%
def label1(element):
    if element > 1:
        return 'High'
    else:
        return 'Low'

# Map values of Series according to input correspondence
economy_impact_map = happiness2015['Economy'].map(label1)

# method to apply a function with additional arguments element-wise
economy_impact_apply1 = happiness2015['Economy'].apply(label1) 
equal = economy_impact_map.equals(economy_impact_apply1)
print(economy_impact_map)

#%%
# define element and x
def label2(element, x):
    if element > x:
        return 'High'
    else:
        return 'Low'
economy_impact_apply2 = happiness2015['Economy'].apply(label2, x = 0.8)

#%%
economy_apply = happiness2015['Economy'].apply(label1)

factors = ['Economy', 'Family', 'Health', 'Freedom', 'Trust', 'Generosity']

# apply functions element-wise to multiple columns at once
factors_impact = happiness2015[factors].applymap(label1)

#%%
def v_counts(col):
    num = col.value_counts()
    den = col.size
    return num/den

v_counts_pct = factors_impact.apply(v_counts)
print(v_counts_pct)

#%%
#Calculate the sum of the factor columns in each row.
happiness2015['Factors Sum'] = happiness2015[['Economy', 'Family', 'Health', 'Freedom', 'Trust', 'Generosity', 'Dystopia Residual']].sum(axis=1)

#Display the first five rows of the result and the Happiness Score column.
happiness2015[['Happiness Score', 'Factors Sum']].head()

#%%
factors = ['Economy', 'Family', 'Health', 'Freedom', 'Trust', 'Generosity', 'Dystopia Residual']

def percentages(col):
    div = col/happiness2015['Happiness Score']
    return div*100
factor_percentages = happiness2015[factors].apply(percentages)

#%%
main_cols = ['Country', 'Region', 'Happiness Rank', 'Happiness Score']
factors = ['Economy', 'Family', 'Health', 'Freedom', 'Trust', 'Generosity', 'Dystopia Residual']

# id_vars = the name of the columns that should remain in the same result
# value_vars = the name of the columns that should be changed to rows in the result

melt = pd.melt(happiness2015, id_vars=main_cols, value_vars=factors)
melt['Percentage'] = round(100*melt['value']/melt['Happiness Score'],2)
print(melt)

#%%
pv_melt = melt.pivot_table(index='variable', values='value')
pv_melt.plot(kind='pie', y='value', legend=False)

#%%
#Concatenate happiness2015, happiness2016, and happiness2017.
combined = pd.concat([happiness2015, happiness2016, happiness2017])

#Create a pivot table listing the mean happiness score for each year. Since the default aggregation function is the mean, we excluded the `aggfunc` argument.
pivot_table_combined = combined.pivot_table(index = 'Year', values = 'Happiness Score')

#Plot the pivot table.
pivot_table_combined.plot(kind ='barh', title='Mean Happiness Scores by Year', xlim = (0,10))

#%% [markdown]
# | Method | Series or Dataframe Method | Applies Functions Element-wise? |
# | --- | --- | --- |
# | Map | Series | Yes |
# | Apply | Series | Yes |
# | Applymap | Dataframe | Yes |
# | Apply | Dataframe | No, applies functions along an axis |

#%% [markdown]
# ## Working With Strings In Pandas

# | Method | Description |
# | --- | ---|
# | Series.str.split()|Splits each element in the Series.|
# | Series.str.strip()|Strips whitespace from each string in the Series.|
# | Series.str.lower()|Converts strings in the Series to lowercase.|
# | Series.str.upper()|Converts strings in the Series to uppercase.|
# | Series.str.get()|Retrieves the ith element of each element in the Series.|
# | Series.str.replace()|Replaces a regex or string in the Series with another string.|
# | Series.str.cat()|Concatenates strings in a Series.|
# | Series.str.extract()|Extracts substrings from the Series matching a regex pattern.|

# Find specific strings or substrings in a column:
#
# `df[col_name].str.contains(pattern)`
#
# Extract specific strings or substrings in a column:
#
# `df[col_name].str.extract(pattern)`
#
# Extract more than one group of patterns from a column:
#
# `df[col_name].str.extractall(pattern)`
#
# Replace a regex or string in a column with another string:
#
# `df[col_name].str.replace(pattern, replacement_string)`

#%%
world_dev = pd.read_csv("World_dev.csv")
col_renaming = {'SourceOfMostRecentIncomeAndExpenditureData': 'IESurvey'}
world_dev = world_dev.rename(col_renaming, axis = 1)

merged = pd.merge(left=happiness2015, right=world_dev, how='left', left_on=happiness2015['Country'], right_on=world_dev['ShortName'])

#%%
def extract_last_word(element):
    element = str(element)
    element = element.split()
    return element[-1]

merged['Currency Apply'] = merged['CurrencyUnit'].apply(extract_last_word)
merged['Currency Apply'].head(5)

#%%
merged['Currency Vectorized'] = merged['CurrencyUnit'].str.split().str.get(-1)
merged['Currency Vectorized'].head(5)

#%%
lengths = merged['CurrencyUnit'].str.len()
value_counts = lengths.value_counts(dropna=False)

# return the length of each currency unit
def lengths_str(element):
    return len(str(element))
lengths_apply1 = merged['CurrencyUnit'].apply(lengths_str)

# exclude the missing values
def lengths_missing(element):
    if pd.isnull(element):
        pass
    else:
        return len(str(element))
lengths_apply2 = merged['CurrencyUnit'].apply(lengths_missing)

#%%
pattern = r"[Nn]ational accounts"
national_accounts = merged['SpecialNotes'].str.contains(pattern)
national_accounts.head(5)

#%%
#Return the value counts for each value in the Series, including missing values.
pattern = r"[Nn]ational accounts"
national_accounts = merged['SpecialNotes'].str.contains(pattern, na=False)
merged_national_accounts = merged[national_accounts]
merged_national_accounts.head()

#%%
pattern = r"([1-2][0-9]{3})"
pattern =r"([0-9]{4})"
years = merged['SpecialNotes'].str.extract(pattern)

#%%
# syntax `(?P<Column_Name>...)`
# caturing the column `Years`
pattern = r"(?P<Years>[1-2][0-9]{3})"
years = merged['IESurvey'].str.extractall(pattern)
value_counts = years['Years'].value_counts()
print(value_counts)

#%%
pattern = r"(?P<First_Year>[1-2][0-9]{3})/?(?P<Second_Year>[0-9]{2})?"
years = merged['IESurvey'].str.extractall(pattern)
first_two_year = years['First_Year'].str[0:2]
years['Second_Year'] = first_two_year + years['Second_Year']

#%%
import matplotlib.pyplot as plt
merged['IncomeGroup'] = merged['IncomeGroup'].str.replace(' income', '').str.replace(':', '').str.upper()
pv_incomes = merged.pivot_table(values='Happiness Score', index='IncomeGroup')
pv_incomes.plot(kind='bar', rot=30, ylim=(0,10))
plt.show()

#%% [markdown]
# ## Working With Missing And Duplicate Data
# Although there is no perfect way to handle missing values and each situation is different, now we know the basic techniques and built some intuition around them to better inform our decisions. Below is the workflow we used to clean missing values:

# * Check for errors in data cleaning/transformation.
# * Use data from additional sources to fill missing values.
# * Drop row/column.
# * Fill missing values with reasonable estimates computed from the available data.
#
#
# We also started to set a more defined data cleaning workflow, in which we:
# * Set a goal for the project.
# * Researched and tried to understand the data.
# * Determined what data was needed to complete our analysis.
# * Added columns.
# * Cleaned specific data types.
# * Combined data sets.
# * Removed duplicate values.
# * Handled the missing values.

#%%
# find the total missing value in each dataset
missing_2015 = happiness2015.isnull().sum()
missing_2016 = happiness2016.isnull().sum()
missing_2017 = happiness2017.isnull().sum()

#%%
# cleaning the name of the column
happiness2017.columns = happiness2017.columns.str.replace('.', ' ').str.replace('\s+', ' ').str.strip().str.upper()
happiness2016.columns = happiness2016.columns.str.replace(r'[\(\)]', '').str.strip().str.upper()
happiness2015.columns = happiness2015.columns.str.replace(r'[\(\)]', '').str.strip().str.upper()


combined = pd.concat([happiness2015, happiness2016, happiness2017], ignore_index = True)
missing = combined.isnull().sum()
print(missing)

#%%
# graph the location of the missing value
import seaborn as sns
combined_updated = combined.set_index('YEAR')
sns.heatmap(combined_updated.isnull(), cbar=False)

#%% [markdown]
# We can make the following observations:
# * No values are missing in the `COUNTRY` column.
# * There are some rows in the 2015, 2016, and 2017 data with missing values in all columns EXCEPT the `COUNTRY` column.
# * Some columns only have data populated for one year.
# * It looks like the `REGION` data is missing for the year 2017.

#%%
regions_2017 = combined[combined['YEAR'] == 2017]['REGION']
missing_region = regions_2017.isnull().sum()
print(missing_region)

#%%
combined = pd.merge(left = combined, right = regions, on = 'COUNTRY', how = 'left')
combined = combined.drop('REGION_x', axis = 1)
combined.isnull().sum()

#%%
# finding the duplication
combined['COUNTRY'] = combined['COUNTRY'].str.upper()
dups = combined.duplicated(['COUNTRY','YEAR'])
combined[dups]

#%%
# dropping the duplication
combined['COUNTRY'] = combined['COUNTRY'].str.upper()
combined = combined.drop_duplicates(['COUNTRY','YEAR'])

#%%
# dropping unused column
columns_to_drop = ['LOWER CONFIDENCE INTERVAL', 'STANDARD ERROR', 'UPPER CONFIDENCE INTERVAL', 'WHISKER HIGH', 'WHISKER LOW']

combined = combined.drop(columns_to_drop,axis = 1)
# dropping any columns that have less than 159 non null value
combined = combined.dropna(thresh = 159, axis = 1)

#%%
sorted = combined.set_index('REGION').sort_values(['REGION', 'HAPPINESS SCORE'])
sns.heatmap(sorted.isnull(), cbar=False)

#%% [markdown]
# From the graph, we confirmed:
# * Only about 4 percent of the values in each column are missing.
# * Dropping rows with missing values won't cause us to lose information in other columns.
#
#
# There are many options for choosing the replacement value, including:
# * A constant value
# * The mean of the column
# * The median of the column
# * The mode of the column

#%%
happiness_mean = combined['HAPPINESS SCORE'].mean()
print(happiness_mean)

combined['HAPPINESS SCORE UPDATED'] = combined['HAPPINESS SCORE'].fillna(happiness_mean)

# the replacing missing values with the Series mean doesn't change the mean of the Series.
print(combined['HAPPINESS SCORE UPDATED'].mean())

#%%
combined = combined.dropna()