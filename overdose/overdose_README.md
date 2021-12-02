# Reference

https://wonder.cdc.gov/ucd-icd10.html  
https://wonder.cdc.gov/controller/saved/D76/D255F416  
https://wonder.cdc.gov/datause.html

.....

Use this function

```    
def clean_overdose_data_file(filepath):
    df = pd.read_csv(filepath, sep = '\t')
    df.drop('Notes', axis=1, inplace=True)
    df.dropna(axis=0, how = 'all', thresh=None, subset=None, inplace=True)
    df.columns = [x.replace(' ','_').replace('/','_') for x in df.columns]
    return(df)
```
