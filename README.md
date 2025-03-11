# PROCESS 

```bash
python prepare_sp.py
```

# Extract Meta data
```bash
python sql_parser_enhanced.py --input sql_raw/arm.GetEmailDetails/arm.GetEmailDetails.sql --output analysis/arm.GetEmailDetails/arm.GetEmailDetails_meta.json
```

# Select stored procedures to analyze 

```bash
python run.py
```

- Wait until the process is finished. 