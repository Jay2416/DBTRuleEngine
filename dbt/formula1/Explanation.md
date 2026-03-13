    # Formula 1 Dataset Documentation

This dataset contains comprehensive Formula 1 race data including drivers, constructors, circuits, and race results. It's structured with fact tables and dimension tables, making it ideal for dbt modeling.

---

## Dataset Overview

The dataset consists of 14 CSV files organized as follows:
- **Dimension Tables**: Contains reference data (drivers, constructors, circuits, seasons)
- **Fact Tables**: Contains transactional/event data (races, results, qualifying, pit stops, lap times)
- **Bridge/Supporting Tables**: Standings and status mappings

---

## Table Schemas

### 1. **drivers.csv** - Dimension Table
Master dimension of all F1 drivers.

| Field | Type | Description |
|-------|------|-------------|
| `driverId` | INT | Primary Key - Unique driver identifier |
| `driverRef` | STRING | Unique driver reference/slug |
| `number` | INT | Driver number (can be NULL if not assigned) |
| `code` | STRING | 3-letter driver code (e.g., HAM, VER) |
| `forename` | STRING | Driver's first name |
| `surname` | STRING | Driver's last name |
| `dob` | DATE | Date of birth |
| `nationality` | STRING | Driver's nationality |
| `url` | STRING | Wikipedia URL reference |

---

### 2. **constructors.csv** - Dimension Table
Master dimension of all F1 teams/constructors.

| Field | Type | Description |
|-------|------|-------------|
| `constructorId` | INT | Primary Key - Unique constructor identifier |
| `constructorRef` | STRING | Unique constructor reference/slug |
| `name` | STRING | Team name |
| `nationality` | STRING | Constructor's nationality |
| `url` | STRING | Wikipedia URL reference |

---

### 3. **circuits.csv** - Dimension Table
Master dimension of all F1 racing circuits/tracks.

| Field | Type | Description |
|-------|------|-------------|
| `circuitId` | INT | Primary Key - Unique circuit identifier |
| `circuitRef` | STRING | Unique circuit reference/slug |
| `name` | STRING | Circuit name |
| `location` | STRING | City/Location name |
| `country` | STRING | Country name |
| `lat` | FLOAT | Latitude coordinate |
| `lng` | FLOAT | Longitude coordinate |
| `alt` | INT | Altitude in meters |
| `url` | STRING | Wikipedia URL reference |

---

### 4. **seasons.csv** - Dimension Table
Master dimension of all F1 seasons/years.

| Field | Type | Description |
|-------|------|-------------|
| `year` | INT | Primary Key - Year of the season |
| `url` | STRING | Wikipedia URL reference for the season |

---

### 5. **status.csv** - Dimension Table
Status dimension for race outcomes (Finished, DNF, etc.).

| Field | Type | Description |
|-------|------|-------------|
| `statusId` | INT | Primary Key - Unique status identifier |
| `status` | STRING | Status description (e.g., "Finished", "Disqualified") |

---

### 6. **races.csv** - Dimension Table
Master dimension of all F1 races.

| Field | Type | Description |
|-------|------|-------------|
| `raceId` | INT | Primary Key - Unique race identifier |
| `year` | INT | Foreign Key - Season year |
| `round` | INT | Race round number in the season |
| `circuitId` | INT | Foreign Key - Circuit identifier |
| `name` | STRING | Race name (e.g., "Australian Grand Prix") |
| `date` | DATE | Race date |
| `time` | TIME | Race start time |
| `url` | STRING | Wikipedia URL reference |
| `fp1_date` | DATE | Free Practice 1 date (nullable) |
| `fp1_time` | TIME | Free Practice 1 time (nullable) |
| `fp2_date` | DATE | Free Practice 2 date (nullable) |
| `fp2_time` | TIME | Free Practice 2 time (nullable) |
| `fp3_date` | DATE | Free Practice 3 date (nullable) |
| `fp3_time` | TIME | Free Practice 3 time (nullable) |
| `quali_date` | DATE | Qualifying date (nullable) |
| `quali_time` | TIME | Qualifying time (nullable) |
| `sprint_date` | DATE | Sprint race date (nullable) |
| `sprint_time` | TIME | Sprint race time (nullable) |

---

### 7. **results.csv** - Fact Table
Race results - core fact table containing driver performance in each race.

| Field | Type | Description |
|-------|------|-------------|
| `resultId` | INT | Primary Key - Unique result identifier |
| `raceId` | INT | Foreign Key - Race identifier |
| `driverId` | INT | Foreign Key - Driver identifier |
| `constructorId` | INT | Foreign Key - Constructor identifier |
| `number` | INT | Driver number for that race |
| `grid` | INT | Grid position (starting position) |
| `position` | INT | Finishing position (nullable if DNF) |
| `positionText` | STRING | Finishing position as text (e.g., "1", "+5.478") |
| `positionOrder` | INT | Position ordering |
| `points` | INT | Points scored in the race |
| `laps` | INT | Number of laps completed |
| `time` | STRING | Race time or gap (e.g., "1:34:50.616" or "+5.478") |
| `milliseconds` | INT | Race time in milliseconds |
| `fastestLap` | INT | Fastest lap number (nullable) |
| `rank` | INT | Fastest lap rank/position (nullable) |
| `fastestLapTime` | STRING | Fastest lap time (e.g., "1:27.452") |
| `fastestLapSpeed` | STRING | Fastest lap speed in km/h |
| `statusId` | INT | Foreign Key - Race status identifier |

---

### 8. **qualifying.csv** - Fact Table
Qualifying session results - driver qualifying performance for each race.

| Field | Type | Description |
|-------|------|-------------|
| `qualifyId` | INT | Primary Key - Unique qualifying result identifier |
| `raceId` | INT | Foreign Key - Race identifier |
| `driverId` | INT | Foreign Key - Driver identifier |
| `constructorId` | INT | Foreign Key - Constructor identifier |
| `number` | INT | Driver number for that race |
| `position` | INT | Qualifying position |
| `q1` | STRING | Q1 best lap time (nullable if eliminated) |
| `q2` | STRING | Q2 best lap time (nullable if eliminated) |
| `q3` | STRING | Q3 best lap time (nullable if eliminated) |

---

### 9. **lap_times.csv** - Fact Table
Detailed lap-by-lap data for each race (large table).

| Field | Type | Description |
|-------|------|-------------|
| `raceId` | INT | Foreign Key - Race identifier |
| `driverId` | INT | Foreign Key - Driver identifier |
| `lap` | INT | Lap number |
| `position` | INT | Position during this lap |
| `time` | STRING | Lap time (e.g., "1:38.109") |
| `milliseconds` | INT | Lap time in milliseconds |

---

### 10. **pit_stops.csv** - Fact Table
Pit stop data for each race.

| Field | Type | Description |
|-------|------|-------------|
| `raceId` | INT | Foreign Key - Race identifier |
| `driverId` | INT | Foreign Key - Driver identifier |
| `stop` | INT | Stop sequence number (1st stop, 2nd stop, etc.) |
| `lap` | INT | Lap number when pit stop occurred |
| `time` | TIME | Time of pit stop (clock time) |
| `duration` | STRING | Duration of pit stop in seconds (e.g., "26.898") |
| `milliseconds` | INT | Pit stop duration in milliseconds |

---

### 11. **sprint_results.csv** - Fact Table
Sprint race results (newer addition to F1 races starting 2021).

| Field | Type | Description |
|-------|------|-------------|
| `resultId` | INT | Primary Key - Unique sprint result identifier |
| `raceId` | INT | Foreign Key - Race identifier |
| `driverId` | INT | Foreign Key - Driver identifier |
| `constructorId` | INT | Foreign Key - Constructor identifier |
| `number` | INT | Driver number for that race |
| `grid` | INT | Starting grid position for sprint |
| `position` | INT | Final position (nullable if DNF) |
| `positionText` | STRING | Final position as text |
| `positionOrder` | INT | Position ordering |
| `points` | INT | Points scored in sprint |
| `laps` | INT | Number of laps completed |
| `time` | STRING | Sprint time or gap |
| `milliseconds` | INT | Sprint time in milliseconds |
| `fastestLap` | INT | Fastest lap number (nullable) |
| `fastestLapTime` | STRING | Fastest lap time |
| `statusId` | INT | Foreign Key - Race status identifier |

---

### 12. **driver_standings.csv** - Fact Table (Slowly Changing Dimension)
Driver championship standings after each race.

| Field | Type | Description |
|-------|------|-------------|
| `driverStandingsId` | INT | Primary Key - Unique standings record identifier |
| `raceId` | INT | Foreign Key - Race identifier (standings after this race) |
| `driverId` | INT | Foreign Key - Driver identifier |
| `points` | INT | Championship points accumulated |
| `position` | INT | Current standings position |
| `positionText` | STRING | Position as text |
| `wins` | INT | Number of race wins |

---

### 13. **constructor_standings.csv** - Fact Table (Slowly Changing Dimension)
Constructor/Team championship standings after each race.

| Field | Type | Description |
|-------|------|-------------|
| `constructorStandingsId` | INT | Primary Key - Unique standings record identifier |
| `raceId` | INT | Foreign Key - Race identifier (standings after this race) |
| `constructorId` | INT | Foreign Key - Constructor identifier |
| `points` | INT | Championship points accumulated |
| `position` | INT | Current standings position |
| `positionText` | STRING | Position as text |
| `wins` | INT | Number of race wins |

---

### 14. **constructor_results.csv** - Fact Table
Constructor points per race (can have multiple entries per constructor per race).

| Field | Type | Description |
|-------|------|-------------|
| `constructorResultsId` | INT | Primary Key - Unique record identifier |
| `raceId` | INT | Foreign Key - Race identifier |
| `constructorId` | INT | Foreign Key - Constructor identifier |
| `points` | INT | Points scored by this constructor in the race |
| `status` | STRING | Status code or additional info (nullable) |

---

## Data Model Architecture

### Dimensional Model Structure

```
                    ┌─────────────┐
                    │   drivers   │
                    └─────────────┘
                          │
                    ┌─────┴─────┐
                    │           │
            ┌──────────────────────────┐
            │      results (fact)       │
            └──────────────────────────┘
                    │
        ┌───────────┼───────────┬──────────────┐
        │           │           │              │
   ┌────────────┐ ┌──────────┐ ┌────────┐ ┌──────────┐
   │constructors│ │ statuses │ │  races │ │ circuits │
   └────────────┘ └──────────┘ └────────┘ └──────────┘
        │
    ┌───┴───┐
    │       │
┌──────────────────────────────────────┐  ┌──────────────┐
│ constructor_standings (bridge fact)  │  │  seasons     │
└──────────────────────────────────────┘  └──────────────┘
```

### Key Relationships

1. **results** - Main fact table connecting:
   - drivers → driverId
   - constructors → constructorId
   - races → raceId
   - status → statusId

2. **qualifying** - Secondary fact table with same foreign keys as results

3. **lap_times & pit_stops** - Detailed fact tables at lap/stop level

4. **driver_standings & constructor_standings** - SCD Type 2 tables tracking cumulative points after each race

---

## Data Types Guide for dbt

### For Creating dbt Models:

**Primary Keys (cast as INT):**
- driverId, constructorId, circuitId, raceId, resultId, qualifyId, statusId

**Foreign Keys (cast as INT):**
- All columns ending in Id that reference other tables

**Dimensions/Attributes (cast as STRING):**
- names, codes, references, nationalities, URLs, status descriptions

**Measures/Numerics:**
- `INT`: points, grid, position, laps, wins, round, year, lap numbers
- `FLOAT`: lat, lng, alt, fastestLapSpeed
- `STRING`: times (keep as string or parse to TIME/INTERVAL as needed)

**Dates/Times:**
- `DATE`: dob, race_date, qualifying dates, practice dates
- `TIME`: race_time, pit_stop_time, qualifying times
- `milliseconds`: Store numeric milliseconds in INT, can convert to intervals

### Nullable Fields (Often NULL, denoted by \N in CSV):
- Driver number (some drivers may not have assigned numbers)
- Qualifying times (drivers eliminated in Q1/Q2/Q3)
- Race position (DNF - Did Not Finish)
- Practice session times (if not held)
- Sprint dates/times (only for races with sprint format)
- Pit stop data (only for drivers who pitted)
- Fastest lap (only if completed distance)

---

## Common dbt Modeling Patterns

### Suggested dbt Structure:

```
models/
├── staging/
│   ├── stg_drivers.sql
│   ├── stg_constructors.sql
│   ├── stg_circuits.sql
│   ├── stg_races.sql
│   ├── stg_results.sql
│   ├── stg_qualifying.sql
│   ├── stg_lap_times.sql
│   └── stg_pit_stops.sql
├── marts/
│   ├── dim_drivers.sql
│   ├── dim_constructors.sql
│   ├── dim_circuits.sql
│   ├── dim_time.sql
│   ├── fct_race_results.sql
│   ├── fct_qualifying.sql
│   └── fct_lap_details.sql
└── reports/
    ├── driver_season_summary.sql
    └── constructor_season_summary.sql
```

---

## Key Insights for dbt Development

1. **ID Columns**: All IDs start from 1 and increment (good for surrogate keys)
2. **Time Data**: Mixed format (times in strings, milliseconds in INT) - normalize in staging layer
3. **NULL Values**: Represented as `\N` in CSV - ensure proper NULL handling in dbt
4. **Slowly Changing Dimensions**: driver_standings and constructor_standings track historical position changes
5. **Large Tables**: lap_times (589K+ rows) and results (26K+ rows) - optimize with proper indexing
6. **Fact Tables**: Multiple grain levels - results (race-driver), lap_times (race-driver-lap), pit_stops (race-driver-stop)
