# LinkFire

This repository is responsible for:

- Process one line at a time, pretending that you're processing a stream.
- Whenever there is a convvalue, use the rates.json and convvalueunit to convert the value to USD.
  Write that value to a new field convusdvalue.
- Split records by values in the type field and create one output file for each type.
- Validate that the linkid is a valid UUID, using the standard library. 
  Invalid records must go to their own output file, e.g. "deadletters.json".


Install the required packages
------------------------------
    $ pip install -r requirements.txt


How to run the code
-------------------
<code>
    $ python -m LinkFire data.json
</code>

