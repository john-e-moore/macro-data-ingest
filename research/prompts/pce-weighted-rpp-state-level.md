This prompt will describe a research task that involves querying national- and state-level regional price parities (RPPs), doing some analysis, and putting together outputs. You can answer these using the database view 'serving.v_state_rpp_pce_weighted_annual'.

Questions:

1. For each RPP category (all items, housing, goods, utilities, other services), what is the national RPP for the most recent year in the data? What would the national RPP be without California? What would the national RPP be without California, New York, New Jersey, Illinois, and Connecticut? Express the differences in terms of RPP as well as percentage change. For instance, if the national RPP of utilities is 100, but without CA (weighted) it is 97.5, that means utilities prices would be 2.5% lower.

2. Individually for each state -- for California, New York, New Jersey, Illinois, and Connecticut -- how have their RPPs for each category changed over the most recent 5 years?


Outputs:

1. Bar charts, one for each RPP category. Each chart should have three bars -- one 'National', one 'Without CA', one 'Without CA, NY, NJ, IL, CT'.

2. Make a multiple line chart for each state, with each line representing one of the RPP categories, going back 5 years. PNG files. Make them look nice, no sloppy or ugly titles, axes, etc. Also give me the data in a single .csv.

3. Write a summary of the findings and save that in a markdown file.

Save outputs to the 'research/outputs/' directory.