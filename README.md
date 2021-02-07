# ODX

## Preprocessing AFC data

```
python preprocessing/process_metro_afc.py <path/to/metro/afc.csv>
```

```
python preprocessing/process_carris_afc.py <path/to/carris/afc.csv>
```

## Preprocessing schedule data

```
python preprocessing/process_carris_schedule.py <path/to/carris/schedule.xlsx>
```

## Combine AFC datasets into single dataset

```
python preprocessing/combine_afc.py -sd 7-10-2019 -ed 15-10-2019 -st 04 -t 03:59
```