import pandas as pd
filepath = "/Users/praneeth/Downloads/term_project"
# Load the data from a CSV file into a dataframe
df = pd.read_csv(filepath+'/sampled.csv')  # Replace 'path_to_your_file.csv' with the actual file path
# print(df.columns())
# Creating the first dataframe with columns 'emotion' and 'tweet'
df_first = df[['emotion', 'tweet']]
df_first.to_csv(filepath+'df_orig.csv', index=False)  # Save to CSV without the index column

# Creating the second dataframe with columns 'emotion' and 'tweet_no_emojis'
df_second = df[['emotion', 'tweet_no_emojis']]
df_second.to_csv(filepath+'df_noemo.csv', index=False)  # Save to CSV without the index column

# Creating the third dataframe with columns 'emotion' and 'tweet_unicode_emojis'
df_third = df[['emotion', 'tweet_unicode_emojis']]
df_third.to_csv(filepath+'df_uni.csv', index=False)  # Save to CSV without the index column

# Creating the fourth dataframe with columns 'emotion', 'tweet', and 'emoticons'
df_fourth = df[['emotion', 'tweet', 'emoticons']]
df_fourth.to_csv(filepath+'df_specchar.csv', index=False)  # Save to CSV without the index column

df_fifth = df[['emotion', 'tweet_no_emojis', 'emoticons']]
df_fifth.to_csv(filepath+'emotion.csv', index=False) 

print("Dataframes have been saved to CSV files successfully.")
