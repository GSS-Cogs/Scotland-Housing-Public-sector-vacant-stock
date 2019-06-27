# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.4'
#       jupytext_version: 1.1.1
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# Housing Statistics for Scotland - Public sector vacant stock

# +
from gssutils import *

scraper = Scraper('https://www2.gov.scot/Topics/Statistics/Browse/Housing-Regeneration/HSfS/VacantStock')
scraper
# -

tabs = {tab.name: tab for tab in scraper.distribution().as_databaker()}
tabs.keys()

tabs = scraper.distribution().as_databaker()

tenure = ['public-sector-vacant-stock',
          'public-sector-vacant-stock-used-as-temporary-accommodation-for-homeless',
          'public-sector-vacant-stock-awaiting-demolition',
          'public-sector-vacant-stock-forming-part-of-a-modernisation-scheme',
          'public-sector-vacant-stock-in-low-demand-areas',
          'public-sector-vacant-stock-other-types'
          ]

tidy = pd.DataFrame()
for i in range(3,9):
    print(i)
    tab = tabs[i]
    cell = tab.filter(contains_string('Public'))
    tl = cell.shift(0,1)
    area = tab.excel_ref('A1').expand(DOWN).is_not_blank().is_not_whitespace() -cell - tl
    year = tl.shift(0,1).fill(RIGHT).is_not_blank().is_not_whitespace()
    observations = area.fill(RIGHT).is_not_blank().is_not_whitespace()
    Dimensions = [
                HDim(year,'Year',DIRECTLY,ABOVE),
                HDim(tl,'Vacancy length',CLOSEST,ABOVE),
                HDim(area,'Area',DIRECTLY,LEFT),
                HDimConst('Measure Type', 'Count'),
                HDimConst('Unit', 'dwellings')
                ]
    c1 = ConversionSegment(observations, Dimensions, processTIMEUNIT=True)    
    table = c1.topandas()
    table['Tenure'] = tenure[i-3]
    tidy = pd.concat([tidy , table])  

import numpy as np
tidy['OBS'].replace('', np.nan, inplace=True)
tidy.dropna(subset=['OBS'], inplace=True)
if 'DATAMARKER' in tidy.columns:
    tidy.drop(columns=['DATAMARKER'], inplace=True)
tidy.rename(columns={'OBS': 'Value', 'Area':'Geography' }, inplace=True)
tidy['Value'] = tidy['Value'].astype(int)

tidy.rename(columns={'Year': 'Period'}, inplace=True)
tidy = tidy[tidy['Period'] != '']

tidy['Period'] = 'day/' + tidy['Period'].astype(str).str[:4] + '-03-31'

tidy['Vacancy length'] = tidy['Vacancy length'].map(
    lambda x: {
        'all vacant stock' : 'total', 'vacant for under 2 weeks' : 'under-2-weeks',
       'vacant for 2 to 6 weeks' : '2-to-6-weeks', 'vacant for 6 to 26 weeks' : '6-to-26-weeks',
       'vacant for 26 weeks to 2 years' : '26-weeks-to-2-years', 
        'vacant for longer than 2 years' : 'longer-than-2-years',
       'length of vacancy unknown' : 'unknown',
       'all vacant stock used as temporary accommodation for homeless' : 'total',
       'all vacant stock awaiting demolition' : 'total',
       'all vacant stock forming part of a modernisation scheme' :'total',
       'all vacant stock in low demand areas' : 'total',
       'all vacant stock of other types' : 'total'
        }.get(x, x))

tidy = tidy[tidy['Geography'] != 'Local Authorities']
tidy = tidy[tidy['Geography'] != 'Scottish Homes']

tidy['Geography'] = tidy['Geography'].map(
    lambda x: {
        'Scottish Borders, The' : 'Scottish Borders', 
        'Shetland' : 'Shetland Islands',
        'Na h-Eilanan Siar' : 'Na h-Eileanan Siar',
        'Orkney' : 'Orkney Islands'
        }.get(x, x))

scotland_gss_codes = pd.read_csv('scotland-gss.csv', index_col='Area')
tidy['Geography'] = tidy['Geography'].map(
    lambda x: scotland_gss_codes.loc[x]['Code']
)

tidy = tidy[['Period','Geography','Vacancy length','Tenure','Measure Type','Value','Unit']]

out = Path('out')
out.mkdir(exist_ok=True)
tidy.to_csv(out / 'observations.csv', index = False)

# +
scraper.dataset.family = 'housing'
scraper.dataset.theme = THEME['housing-planning-local-services']
with open(out / 'dataset.trig', 'wb') as metadata:
    metadata.write(scraper.generate_trig())
    
schema = CSVWSchema('https://ons-opendata.github.io/ref_housing/')
schema.create(out / 'observations.csv', out / 'observations.csv-schema.json')
# -

tidy


